from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException, AirflowException

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
import sys

def _extract(connection_id, table_name, incremental, date = None):
    pg_hook = PostgresHook(postgres_conn_id = connection_id)
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    query = f"SELECT * FROM {table_name}"
    if incremental and table_name == 'staging.order_status_analytic':
        raise AirflowSkipException("order_status_analytic doesn't have new data. Skipped...")
    
    if incremental and table_name != 'staging.order_status_analytic':
        query += f" WHERE created_at::DATE = '{date}'::DATE - INTERVAL '1 DAY';"

    cursor.execute(query)
    records = cursor.fetchall()
    column_list = [desc[0] for desc in cursor.description]

    cursor.close()
    connection.commit()
    connection.close()

    # if not records:
    #     raise AirflowSkipException(f"{table_name} doesn't have new data. Skipped...")
    
    return column_list, records