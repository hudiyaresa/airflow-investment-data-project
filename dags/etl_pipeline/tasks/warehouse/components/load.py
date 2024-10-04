from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException

from helper.minio import CustomMinio
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import timedelta

class Load:
    def _warehouse(table_name, incremental, table_pkey, date):
        try:
            if incremental:
                object_name = f'{table_name}-{(pd.to_datetime(date) - timedelta(days=1)).strftime("%Y-%m-%d")}.csv'
            else:
                object_name = f'{table_name}.csv'

            bucket_name = 'valid-data'
            df = CustomMinio._get_spark_dataframe(bucket_name, object_name)

            df = df.withColumnRenamed(table_pkey, 'id')

            pg_hook = PostgresHook(postgres_conn_id='warehouse_db')
            connection = pg_hook.get_conn()
            cursor = connection.cursor()

            for row in df.collect():
                cursor.execute(f"""
                    INSERT INTO public.{table_name} ({', '.join(df.columns)})
                    VALUES ({', '.join(['%s'] * len(df.columns))})
                    ON CONFLICT (id) DO UPDATE SET
                    {', '.join([f"{col} = EXCLUDED.{col}" for col in df.columns if col != 'id'])}
                """, tuple(row))

            connection.commit()
            cursor.close()
            connection.close()
        except:
            raise AirflowSkipException(f"{table_name} doesn't have new data. Skipped...")