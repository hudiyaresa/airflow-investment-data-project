from airflow.exceptions import AirflowSkipException, AirflowException
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from helper.minio import CustomMinio
from datetime import timedelta
from airflow.models import Variable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

import pandas as pd
import requests
import gspread
import sys

BASE_PATH = "/opt/airflow/dags"

class Extract:
    """
    A class used to extract data from various sources such as databases, APIs, and spreadsheets.
    """

    @staticmethod
    def _dellstore_db(table_name, incremental):
        """
        Extract data from Dellstore database.

        Args:
            table_name (str): Name of the table to extract data from.
            incremental (bool): Flag to indicate if the extraction is incremental.

        Raises:
            AirflowException: If there is an error during extraction.
        """
        try:
            # Initialize Spark session
            spark = SparkSession.builder \
                .appName(f"Extract from source - {table_name}") \
                .getOrCreate()

            # Define query and object name based on incremental flag
            query = f"(SELECT * FROM {table_name}) as data"
            object_name = f'/dellstore-db/{table_name}'

            if incremental:
                date = '{{ds}}'
                query = f"(SELECT * FROM {table_name} WHERE created_at::DATE = '{date}'::DATE - INTERVAL '1 DAY') as data"
                object_name = f'/dellstore-db/{table_name}-{(pd.to_datetime(date) - timedelta(days=1)).strftime("%Y-%m-%d")}'

            # Read data from database
            df = spark.read.jdbc(
                url="jdbc:postgresql://dellstore_db:5432/dellstore",
                table=query,
                properties={
                    "user": "postgres",
                    "password": "postgres",
                    "driver": "org.postgresql.Driver"
                }
            )

            # Check if DataFrame is empty
            if df.isEmpty():
                spark.stop()
                print(f"{table_name} doesn't have new data. Skipped...")
                return

            # Clean string columns by replacing newline characters
            bucket_name = 'extracted-data'
            for col_name, col_type in df.dtypes:
                if col_type == 'string':
                    df = df.withColumn(col_name, regexp_replace(col(col_name), '\n', ' '))

            # Write DataFrame to S3 in CSV format
            df.write \
                .format("csv") \
                .option("header", "true") \
                .option("delimiter", ";") \
                .mode("overwrite") \
                .save(f"s3a://{bucket_name}/{object_name}")

            # Stop Spark session
            spark.stop()

        except Exception as e:
            raise AirflowException(f"Error when extracting {table_name} : {str(e)}")

    @staticmethod
    def _dellstore_api(ds):
        """
        Extract data from Dellstore API.

        Args:
            ds (str): Date string.

        Raises:
            AirflowException: If failed to fetch data from Dellstore API.
            AirflowSkipException: If no new data is found.
        """
        try:
            # Fetch data from API
            response = requests.get(
                url=Variable.get('dellstore_api_url'),
                params={"start_date": ds, "end_date": ds},
            )

            # Check response status
            if response.status_code != 200:
                raise AirflowException(f"Failed to fetch data from Dellstore API. Status code: {response.status_code}")

            # Parse JSON data
            json_data = response.json()
            if not json_data:
                raise AirflowSkipException("No new data in Dellstore API. Skipped...")

            # Save JSON data to S3
            bucket_name = 'extracted-data'
            object_name = f'/dellstore-api/data-{(pd.to_datetime(ds) - timedelta(days=1)).strftime("%Y-%m-%d")}.json'
            CustomMinio._put_json(json_data, bucket_name, object_name)
        
        except AirflowSkipException as e:
            raise e
        
        except AirflowException as e:
            raise e
        
        except Exception as e:
            raise AirflowException(f"Error when extracting Dellstore API: {str(e)}")

    @staticmethod
    def _dellstore_spreadsheet():
        """
        Extract data from Dellstore spreadsheet.

        Raises:
            AirflowSkipException: If no data is found.
            AirflowException: If failed to extract data from Dellstore spreadsheet.
        """
        try:
            # Initialize Google Sheets client
            hook = GoogleBaseHook(gcp_conn_id="dellstore_analytics")
            credentials = hook.get_credentials()
            google_credentials = gspread.Client(auth=credentials)

            # Open spreadsheet and get data
            sheet = google_credentials.open("dellstore_analytic")
            worksheet = sheet.get_worksheet(0)
            df = pd.DataFrame(worksheet.get_all_records())

            # Check if DataFrame is empty
            if df.empty:
                raise AirflowSkipException("No data in Dellstore Analytics Spreadsheets. Skipped...")

            # Save DataFrame to S3 in CSV format
            CustomMinio._put_csv(df, 'extracted-data', f'/dellstore-spreadsheet/data.csv')
        
        except AirflowSkipException as e:
            raise e
        
        except Exception as e:
            raise AirflowException(f"Error when extracting Dellstore Analytics Spreadsheets: {str(e)}")

if __name__ == "__main__":
    """
    Main entry point for the script. Extracts data from Dellstore database based on command line arguments.
    """
    if len(sys.argv) != 3:
        sys.exit(-1)

    table_name = sys.argv[1]    
    incremental = sys.argv[2].lower() == 'true'

    Extract._dellstore_db(table_name, incremental)