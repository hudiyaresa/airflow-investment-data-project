from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException

import pandas as pd
from helper.minio import CustomMinio
from sqlalchemy import create_engine
from pangres import upsert
from datetime import timedelta

class Load:
    def _warehouse(table_name, incremental, table_pkey, date):
        try:
            if incremental:
                object_name = f'{table_name}-{(pd.to_datetime(date) - timedelta(days=1)).strftime("%Y-%m-%d")}.csv'
            else:
                object_name = f'{table_name}.csv'

            bucket_name = 'valid-data'
            df = CustomMinio._get_dataframe(bucket_name, object_name)

            df = df.set_index(table_pkey)

            engine = create_engine(PostgresHook(postgres_conn_id = 'warehouse_db').get_uri())

            upsert(
                con = engine,
                df = df,
                table_name = table_name,
                schema = 'public',
                if_row_exists = 'update'
            )

            engine.dispose()
        except:
            raise AirflowSkipException(f"{table_name} doesn't have new data. Skipped...")