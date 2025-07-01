from airflow.decorators import dag, task, task_group
from airflow.operators.python import PythonOperator
from pendulum import datetime
from helper.minio import MinioClient
from helper.postgres import Execute
from investment_data_profiling.tasks.extract import Extract
from investment_data_profiling.tasks.transform_load import TransformLoad

@dag(
    dag_id='investment_data_profiling',
    start_date=datetime(2024, 9, 1),
    schedule='@once',
    catchup=False
)
def investment_data_profiling():
    
    investment_data_create_funct = PythonOperator(
        task_id='investment_create_funct',
        python_callable=Execute._query,
        op_kwargs={
            "connection_id": "investment_db",
            "query_path": "/investment_data_profiling/query/data_profile_quality_func.sql"
        }
    )

    @task
    def create_bucket():
        minio_client = MinioClient._get()
        bucket_name = 'data-profile-quality'
        
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)

    create_investment_profile_table = PythonOperator(
        task_id='create_investment_profile_table',
        python_callable=Execute._query,
        op_kwargs={
            "connection_id": "warhouse_db",
            "query_path": "/investment_data_profiling/query/create_table.sql"
        }
    )

    @task_group
    def profiling_quality_pipeline():
        @task
        def extract():
            Extract._investment_db()

        @task
        def transform_load():
            TransformLoad._investment_db()

        extract() >> transform_load()

    investment_data_create_funct >> create_bucket() >> create_investment_profile_table >> profiling_quality_pipeline()

investment_data_profiling()