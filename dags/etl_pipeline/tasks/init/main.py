from airflow.decorators import task, task_group
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from helper.minio import MinioClient


@task_group
def init():
    @task
    def create_bucket():
        minio_client = MinioClient._get()
        bucket_name = ['extracted-data', 'transformed-data', 'valid-data', 'invalid-data']
        
        for bucket in bucket_name:
            if not minio_client.bucket_exists(bucket):
                minio_client.make_bucket(bucket)
                
    @task_group
    def generate_schema():
        staging = SQLExecuteQueryOperator(
            task_id='staging',
            conn_id="staging_db",
            sql="models/staging.sql"
        )

        warehouse = SQLExecuteQueryOperator(
            task_id='warehouse',
            conn_id="warehouse_db",
            sql="models/warehouse.sql"
        )

        staging >> warehouse

    create_bucket() >> generate_schema()