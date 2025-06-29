from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from pendulum import datetime
from helper.minio import MinioClient
from helper.postgres import Execute

@dag(
    dag_id = 'profiling_quality_init',
    start_date = datetime(2024, 9, 1),
    schedule = "@once",
    catchup = False
)

def profiling_quality_init():
    investment_data_create_funct = PythonOperator(
        task_id = 'investment_create_funct',
        python_callable = Execute._query,
        op_kwargs = {
            "connection_id": "investment_db",
            "query_path": "/profiling_quality_init/query/data_profile_quality_func.sql"
        }
    )

    @task
    def create_bucket():
        minio_client = MinioClient._get()
        bucket_name = 'data-profile-quality'
        
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            
    create_investment_profile_table = PythonOperator(
        task_id = 'create_investment_profile_table',
        python_callable = Execute._query,
        op_kwargs = {
            "connection_id": "profile_quality_db",
            "query_path": "/profiling_quality_init/query/create_table.sql"
        }
    )

    investment_data_create_funct >> create_bucket() >> create_investment_profile_table

profiling_quality_init()