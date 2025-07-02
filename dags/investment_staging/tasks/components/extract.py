from airflow.exceptions import AirflowSkipException, AirflowException
from helper.minio import CustomMinio
from helper.conn import get_jdbc_url
from datetime import timedelta
from airflow.models import Variable
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

import pandas as pd
import requests
import gspread
import sys
import os

load_dotenv()

BASE_PATH = "/opt/airflow/dags"
source_db = get_jdbc_url("source")
source_db_user = os.getenv("INVESTMENT_DB_USER")
source_db_password = os.getenv("INVESTMENT_DB_PASSWORD")

class Extract:
    """
    A class used to extract data from various sources such as databases, APIs, and spreadsheets.
    """

    @staticmethod
    def _investment_db(table_name, incremental, date):
        """
        Extract data from Investment database.

        Args:
            table_name (str): Name of the table to extract data from.
            incremental (bool): Flag to indicate if the extraction is incremental.
            date (str): Date string for incremental extraction.

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
            object_name = f'/investment-db/{table_name}'

            if incremental:
                query = f"(SELECT * FROM {table_name} WHERE created_at::DATE = '{date}'::DATE - INTERVAL '1 DAY') as data"
                object_name = f'/investment-db/{table_name}-{(pd.to_datetime(date) - timedelta(days=1)).strftime("%Y-%m-%d")}'

            # Read data from database
            df = spark.read.jdbc(
                url=source_db,
                table=query,
                properties={
                    "user": source_db_user,
                    "password": source_db_password,
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
            raise AirflowException(f"Error when extracting {table_name}: {str(e)}")

    @staticmethod
    def _investment_api():
        """
        Extract data from Investment API.

        Raises:
            AirflowException: If failed to fetch data from Investment API.
            AirflowSkipException: If no new data is found.
        """
        try:
            # Fetch data from API
            response = requests.get(
                url=Variable.get('investment_api_url')
            )

            # Check response status
            if response.status_code != 200:
                raise AirflowException(f"Failed to fetch data from Investment API. Status code: {response.status_code}")

            # Parse JSON data
            json_data = response.json()
            if not json_data:
                raise AirflowSkipException("No new data in Investment API. Skipped...")

            # Replace newline characters in JSON data
            def replace_newlines(obj):
                if isinstance(obj, dict):
                    return {k: replace_newlines(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [replace_newlines(elem) for elem in obj]
                elif isinstance(obj, str):
                    return obj.replace('\n', ' ')
                else:
                    return obj

            json_data = replace_newlines(json_data)

            # Convert JSON to DataFrame
            df = pd.DataFrame(json_data)

            # Save to CSV in memory
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)            

            # Save JSON data to S3
            bucket_name = 'extracted-data'
            object_name = f'/investment-api/milestone-data.csv'
            CustomMinio._put_file(
                file_data=csv_buffer.getvalue(),
                bucket_name=bucket_name,
                object_name=object_name,
                content_type='text/csv'
            )

        except AirflowSkipException as e:
            raise e

        except AirflowException as e:
            raise e

        except Exception as e:
            raise AirflowException(f"Error when extracting Investment API: {str(e)}")
        

    @staticmethod
    def _investment_csv(data_name):
        try:

            spark = SparkSession.builder \
                .appName(f"Extract CSV - {data_name}") \
                .getOrCreate()

            csv_read = spark.read.csv(f"/opt/airflow/external/{data_name}.csv", header=True)

            df = csv_read.toPandas()            

            # Save to CSV in memory
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)            

            # Save JSON data to S3
            bucket_name = 'extracted-data'
            object_name = f'/investment-csv/{data_name}.csv'
            CustomMinio._put_file(
                file_data=csv_buffer.getvalue(),
                bucket_name=bucket_name,
                object_name=object_name,
                content_type='text/csv'
            )

        except AirflowSkipException as e:
            raise e

        except AirflowException as e:
            raise e

        except Exception as e:
            raise AirflowException(f"Error when extracting Investment API: {str(e)}")     
           
        finally:
            if spark is not None:
                spark.stop()           

if __name__ == "__main__":
    """
    Main entry point for the script. Extracts data from Investment database based on command line arguments.
    """
    if len(sys.argv) != 4:
        sys.exit(-1)

    table_name = sys.argv[1]    
    incremental = sys.argv[2].lower() == 'true'
    date = sys.argv[3]

    Extract._investment_db(table_name, incremental, date)
