from airflow.decorators import task, task_group
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from helper.minio import MinioClient


@task_group
def init():
    @task_group
    def generate_schema():
        stg_generate_schema = SQLExecuteQueryOperator(
            task_id='stg_generate_schema',
            conn_id="staging_db",
            sql="tasks/init/models/staging.sql"
        )

        warehouse_generate_schema = SQLExecuteQueryOperator(
            task_id='warehouse_generate_schema',
            conn_id="warehouse_db",
            sql="tasks/init/models/warehouse.sql"
        )

        stg_generate_schema >> warehouse_generate_schema
    
    @task
    def create_bucket():
        minio_client = MinioClient._get()
        bucket_name = ['extracted-data', 'transformed-data', 'valid-data', 'invalid-data']
        
        for bucket in bucket_name:
            if not minio_client.bucket_exists(bucket):
                minio_client.make_bucket(bucket)

    create_bucket() >> generate_schema()