from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
from pyspark.sql import SparkSession
from datetime import timedelta
from sqlalchemy import create_engine
from pangres import upsert

import pandas as pd
import sys

class Load:
    """
    A class used to load data into the warehouse.
    """

    @staticmethod
    def _warehouse(table_name, table_pkey, incremental, date):
        """
        Load data into the warehouse.

        Args:
            table_name (str): The name of the table to load data into.
            table_pkey (str): The primary key of the table.
            incremental (bool): Flag to indicate if the process is incremental.
            date (str): The date for incremental loading.
        """
        try:
            # Initialize Spark session
            spark = SparkSession.builder \
                .appName(f"Load to warehouse - {table_name}") \
                .getOrCreate()

            # Define bucket and object name
            bucket_name = 'valid-data'
            object_name = f'/{table_name}'

            # Evaluate table_pkey if it is a list
            if table_pkey.startswith('[') and table_pkey.endswith(']'):
                table_pkey = eval(table_pkey)

            # Adjust object name for incremental loading
            if incremental:
                object_name = f'/{table_name}-{(pd.to_datetime(date) - timedelta(days=1)).strftime("%Y-%m-%d")}'

            try:
                # Read data from S3
                df = spark.read.options(
                    delimiter=";", 
                    header=True
                ).csv(f"s3a://{bucket_name}/{object_name}")
                
            except:
                # Stop Spark session if reading data fails
                spark.stop()
                print(f"{table_name} doesn't have new data. Skipped...")
                return

            # Check if DataFrame is empty
            if df.rdd.isEmpty():
                # Stop Spark session if DataFrame is empty
                spark.stop()
                print(f"{table_name} doesn't have new data. Skipped...")
                return

            # Convert Spark DataFrame to Pandas DataFrame
            pandas_df = df.toPandas()
            spark.stop()

            # Set index for Pandas DataFrame
            pandas_df = pandas_df.set_index(table_pkey)

            # Create SQLAlchemy engine and upsert data
            engine = create_engine(PostgresHook(postgres_conn_id='warehouse_db').get_uri())
            upsert(
                con=engine,
                df=pandas_df,
                table_name=table_name.replace('_history', '') if table_name.endswith('_history') else table_name,
                schema='public',
                if_row_exists='update'
            )
        except Exception as e:
            raise AirflowException(f"Error when loading {table_name}: {str(e)}")

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: load.py <table_name> <table_pkey> <incremental> <date>")
        sys.exit(-1)

    table_name = sys.argv[1]
    table_pkey = sys.argv[2]
    incremental = sys.argv[3].lower() == 'true'
    date = sys.argv[4]

    Load._warehouse(
        table_name, 
        table_pkey,
        incremental, 
        date
    )