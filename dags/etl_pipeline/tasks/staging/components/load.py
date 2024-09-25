from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
from airflow import AirflowException
import pandas as pd
from helper.minio import MinioClient, CustomMinio
from sqlalchemy import create_engine
from pangres import upsert
from datetime import timedelta
import json

class Load:
    @staticmethod
    def _dellstore_db(table_name, incremental, **kwargs):
        """
        Load data from Dellstore database into staging area.

        Args:
            table_name (str): Name of the table to load data into.
            incremental (bool): Whether to load incremental data or not.
            **kwargs: Additional keyword arguments.
        """
        try:
            date = kwargs.get('ds')
            table_pkey = kwargs.get('table_pkey')

            object_name = f'/temp/{table_name}-{(pd.to_datetime(date) - timedelta(days=1)).strftime("%Y-%m-%d")}.csv' if incremental else f'/temp/{table_name}.csv'
            bucket_name = 'extracted-data'
            engine = create_engine(PostgresHook(postgres_conn_id='staging_db').get_uri())

            try:
                df = CustomMinio._get_dataframe(bucket_name, object_name)
                df = df.set_index(table_pkey[table_name])

                upsert(
                    con=engine,
                    df=df,
                    table_name=table_name,
                    schema='staging',
                    if_row_exists='update'
                )

            except Exception as e:
                engine.dispose()
                raise AirflowSkipException(f"{table_name} doesn't have new data. Skipped...")

        except Exception as e:
            raise AirflowException(f"Error when loading {table_name} : {str(e)}")

    @staticmethod
    def _dellstore_api(ds):
        """
        Load data from Dellstore API into staging area.

        Args:
            ds (str): Date string for the data to load.
        """
        bucket_name = 'extracted-data'
        object_name = f'/temp/dellstore_api_{(pd.to_datetime(ds) - timedelta(days=1)).strftime("%Y-%m-%d")}.json'

        try:
            engine = create_engine(PostgresHook(postgres_conn_id='staging_db').get_uri())

            try:
                minio_client = MinioClient._get()
                data = minio_client.get_object(bucket_name=bucket_name, object_name=object_name).read().decode('utf-8')
                data = json.loads(data)

                df = pd.json_normalize(data)
                if df.empty:
                    raise AirflowSkipException("Doesn't have new data. Skipped...")

                df = df.set_index(['customer_id', 'order_id', 'orderline_id'])

                upsert(
                    con=engine,
                    df=df,
                    table_name='customer_orders_history',
                    schema='staging',
                    if_row_exists='update'
                )
            except:
                raise AirflowSkipException("Doesn't have new data. Skipped...")
            
        except Exception as e:
            raise AirflowException(f"Error when loading data from Dellstore API: {str(e)}")

    @staticmethod
    def _dellstore_spreadsheet():
        """
        Load data from Dellstore spreadsheet into staging area.
        """
        bucket_name = 'extracted-data'
        object_name = f'/temp/dellstore_analytics.csv'

        try:
            df = CustomMinio._get_dataframe(bucket_name, object_name)
            if df.empty:
                raise AirflowSkipException("Dataframe is empty. Skipped...")

            df = df.set_index('orderid')

            engine = create_engine(PostgresHook(postgres_conn_id='staging_db').get_uri())

            upsert(
                con=engine,
                df=df,
                table_name='order_status_analytic',
                schema='staging',
                if_row_exists='update'
            )
        except:
            raise AirflowSkipException(f"Doesn't have new data. Skipped...")
        finally:
            engine.dispose()