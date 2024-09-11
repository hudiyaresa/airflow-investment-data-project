from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException

import pandas as pd
from helper.minio import MinioClient
from sqlalchemy import create_engine
from pangres import upsert
from datetime import timedelta
import json

class Load:
    def _dellstore_db(table_name, incremental, **kwargs):
        """
        Load data from Dellstore database into staging area.

        Args:
            table_name (str): Name of the table to load data into.
            incremental (bool): Whether to load incremental data or not.
            **kwargs: Additional keyword arguments.
        """
        try:
            date = kwargs['ds']
            table_pkey = kwargs['table_pkey']

            if incremental:
                object_name = f'/temp/{table_name}-{(pd.to_datetime(date) - timedelta(days=1)).strftime("%Y-%m-%d")}.csv'
            else:
                object_name = f'/temp/{table_name}.csv'

            minio_client = MinioClient._get()
            bucket_name = 'extracted-data'

            data = minio_client.get_object(
                bucket_name = bucket_name,
                object_name = object_name
            )

            df = pd.read_csv(data)
            df = df.set_index(table_pkey[table_name])

            engine = create_engine(PostgresHook(postgres_conn_id = 'staging_db').get_uri())

            upsert(
                con = engine,
                df = df,
                table_name = table_name,
                schema = 'staging',
                if_row_exists = 'update'
            )

            engine.dispose()
        except:
            raise AirflowSkipException(f"{table_name} Does'nt exist in bucket")

    def _dellstore_api(ds):
        """
        Load data from Dellstore API into staging area.

        Args:
            ds (str): Date string for the data to load.
        """
        minio_client = MinioClient._get()
        bucket_name = 'extracted-data'
        object_name = f'/temp/dellstore_api_{(pd.to_datetime(ds) - timedelta(days=1)).strftime("%Y-%m-%d")}.json'

        data = minio_client.get_object(
            bucket_name = bucket_name,
            object_name = object_name
        )

        data = data.read().decode('utf-8')
        data = json.loads(data)
        print(data)

        df = pd.json_normalize(data)
        
        if not df.empty:
            df = df.set_index(['customer_id','order_id','orderline_id'])

            engine = create_engine(PostgresHook(postgres_conn_id='staging_db').get_uri())

            upsert(
                con = engine,
                df = df,
                table_name = 'customer_orders_history',
                schema = 'staging',
                if_row_exists = 'update'
            )
        
        else:
            raise AirflowSkipException("Dataframe is empty. Skipped...")
        
    def _dellstore_spreadsheet():
        """
        Load data from Dellstore spreadsheet into staging area.
        """
        minio_client = MinioClient._get()
        bucket_name = 'extracted-data'
        object_name = f'/temp/dellstore_analytics.csv'

        data = minio_client.get_object(
            bucket_name = bucket_name,
            object_name = object_name
        )

        df = pd.read_csv(data)
        df = df.set_index('orderid')

        engine = create_engine(PostgresHook(postgres_conn_id='staging_db').get_uri())

        upsert(
            con = engine,
            df = df,
            table_name = 'order_status_analytic',
            schema = 'staging',
            if_row_exists = 'update'
        )

        engine.dispose()