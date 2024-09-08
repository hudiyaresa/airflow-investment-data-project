from airflow.decorators import dag, task, task_group
from airflow.operators.python import PythonOperator
from pendulum import datetime
from profiling_quality_pipeline.tasks.extract import Extract
from profiling_quality_pipeline.tasks.transform_load import TransformLoad

@dag(
    dag_id = 'profiling_quality_pipeline',
    start_date = datetime(2024, 9, 1),
    schedule = "@daily",
    catchup = False
)
def profiling_quality_pipeline():
    @task_group
    def dellstore_db():
        extract = PythonOperator(
            task_id = 'Extract',
            python_callable = Extract._dellstore_db
        )

        transform_load = PythonOperator(
            task_id = 'transform_Load',
            python_callable = TransformLoad._dellstore_db
        )
  
        extract >> transform_load

    dellstore_db()

profiling_quality_pipeline()