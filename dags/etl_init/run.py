from airflow.decorators import dag, task    
from airflow.operators.python import PythonOperator

from pendulum import datetime
from helper.minio import MinioClient
from helper.postgres import Execute
from etl_pipeline.tasks.dellstore_db import dellstore_db

@dag(
    dag_id = 'etl_init',
    start_date = datetime(2024, 9, 1),
    schedule = "@once",
    catchup = False
)

def etl_init():
    stg_create_table = PythonOperator(
        task_id = 'stg_create_table',
        python_callable = Execute._query,
        op_kwargs = {
            "connection_id": "staging_db",
            "query_path": "/etl_init/query/generate_schema.sql"
        }
    )

    @task
    def create_bucket():
        minio_client = MinioClient._get()
        bucket_name = 'extracted-data'
        
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)


    stg_create_table >> create_bucket() >> dellstore_db()



etl_init()