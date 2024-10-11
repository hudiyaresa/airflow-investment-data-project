from airflow.exceptions import AirflowException, AirflowSkipException
from pyspark.sql import SparkSession
from datetime import timedelta
from pangres import upsert
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
from helper.minio import MinioClient, CustomMinio

import pandas as pd
import sys
import json

class Load:
    """
    A class used to load data into the staging area from various sources such as databases, APIs, and spreadsheets.
    """

    @staticmethod
    def _dellstore_db(table_name, table_pkey, incremental, date):
        """
        Load data from Dellstore database into staging area.

        Args:
            table_name (str): Name of the table to load data into.
            table_pkey (str): Primary key of the table.
            incremental (bool): Flag to indicate if the loading is incremental.
            date (str): Date string for the data to load.
        """
        try:
            # Initialize Spark session
            spark = SparkSession.builder \
                .appName(f"Load to staging - {table_name}") \
                .getOrCreate()

            # Define bucket and object name
            bucket_name = 'extracted-data'
            object_name = f'/dellstore-db/{table_name}/*.csv'

            # Change string list to actual list. e.g: '[orderid]' to ['orderid']
            if table_pkey.startswith('[') and table_pkey.endswith(']'):
                table_pkey = eval(table_pkey)

            # Adjust object name for incremental loading
            if incremental:
                object_name = f'/dellstore-db/{table_name}-{(pd.to_datetime(date) - timedelta(days=1)).strftime("%Y-%m-%d")}'

            try:
                # Read data from S3
                df = spark.read.options(
                    delimiter=";",
                    header=True
                ).csv(f"s3a://{bucket_name}/{object_name}")
            
            except:
                spark.stop()
                print(f"{table_name} doesn't have new data. Skipped...")
                return

            # Convert Spark DataFrame to Pandas DataFrame
            pandas_df = df.toPandas()
            spark.stop()

            # Set index for Pandas DataFrame
            pandas_df = pandas_df.set_index(table_pkey)

            # Create SQLAlchemy engine and upsert data
            engine = create_engine(PostgresHook(postgres_conn_id='staging_db').get_uri())
            upsert(
                con=engine,
                df=pandas_df,
                table_name=table_name,
                schema='staging',
                if_row_exists='update'
            )
            
        except Exception as e:
            raise AirflowException(f"Error when loading {table_name}: {str(e)}")

    @staticmethod
    def _dellstore_api(ds):
        """
        Load data from Dellstore API into staging area.

        Args:
            ds (str): Date string for the data to load.
        """
        bucket_name = 'extracted-data'
        object_name = f'/dellstore-api/data-{(pd.to_datetime(ds) - timedelta(days=1)).strftime("%Y-%m-%d")}.json'

        try:
            # Create SQLAlchemy engine
            engine = create_engine(PostgresHook(postgres_conn_id='staging_db').get_uri())

            try:
                # Get data from Minio
                minio_client = MinioClient._get()
                try:
                    data = minio_client.get_object(bucket_name=bucket_name, object_name=object_name).read().decode('utf-8')
                except:
                    raise AirflowSkipException(f"dellstore_api doesn't have new data. Skipped...")

                # Load data into Pandas DataFrame
                data = json.loads(data)
                df = pd.json_normalize(data)
                df = df.set_index(['customer_id', 'order_id', 'orderline_id'])

                # Upsert data into database
                upsert(
                    con=engine,
                    df=df,
                    table_name='customer_orders_history',
                    schema='staging',
                    if_row_exists='update'
                )
            except AirflowSkipException as e:
                engine.dispose()
                raise e
            
            except Exception as e:
                engine.dispose()
                raise AirflowException(f"Error when loading data from Dellstore API: {str(e)}")
            
        except AirflowSkipException as e:
            raise e
            
        except Exception as e:
            raise e

    @staticmethod
    def _dellstore_spreadsheet():
        """
        Load data from Dellstore spreadsheet into staging area.
        """
        bucket_name = 'extracted-data'
        object_name = f'/dellstore-spreadsheet/data.csv'

        try:
            # Create SQLAlchemy engine
            engine = create_engine(PostgresHook(postgres_conn_id='staging_db').get_uri())

            try:
                # Get data from Minio
                df = CustomMinio._get_dataframe(bucket_name, object_name)
                if df.empty:
                    raise AirflowSkipException("dellstore_spreadsheet doesn't have data. Skipped...")

                # Set index for Pandas DataFrame
                df = df.set_index('orderid')

                # Upsert data into database
                upsert(
                    con=engine,
                    df=df,
                    table_name='order_status_analytic',
                    schema='staging',
                    if_row_exists='update'
                )
            except AirflowSkipException as e:
                engine.dispose()
                raise e
            
        except AirflowSkipException as e:
            raise e
            
        except Exception as e:
            raise AirflowException(f"Error when loading data from Dellstore spreadsheet: {str(e)}")

if __name__ == "__main__":
    """
    Main entry point for the script. Loads data into Dellstore database based on command line arguments.
    """
    if len(sys.argv) != 5:
        sys.exit(-1)

    table_name = sys.argv[1]
    table_pkey = sys.argv[2]
    incremental = sys.argv[3].lower() == 'true'
    date = sys.argv[4]

    Load._dellstore_db(table_name, table_pkey, incremental, date)