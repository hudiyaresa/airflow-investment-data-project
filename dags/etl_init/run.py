from airflow.decorators import dag, task, task_group
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from pendulum import datetime
from helper.minio import MinioClient
from etl_pipeline.tasks.staging.dellstore_db import dellstore_db

@dag(
    dag_id = 'etl_init',
    start_date = datetime(2024, 9, 1),
    schedule = "@once",
    catchup = False
)

def etl_init():
    @task_group
    def generate_schema():
        stg_generate_schema = SQLExecuteQueryOperator(
            task_id='stg_generate_schema',
            conn_id="staging_db",
            sql="query/staging_schema.sql"
        )

        warehouse_generate_schema = SQLExecuteQueryOperator(
            task_id='warehouse_generate_schema',
            conn_id="warehouse_db",
            sql="query/warehouse_schema.sql"
        )

        stg_generate_schema >> warehouse_generate_schema
    
    @task
    def create_bucket():
        minio_client = MinioClient._get()
        bucket_name = ['extracted-data', 'transformed-data', 'valid-data', 'invalid-data']
        
        for bucket in bucket_name:
            if not minio_client.bucket_exists(bucket):
                minio_client.make_bucket(bucket)

    @task_group
    def init_load_stg():
        dellstore_db(incremental = False)

    create_bucket() >> generate_schema() >> init_load_stg()

etl_init()