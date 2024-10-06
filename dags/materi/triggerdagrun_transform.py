from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from datetime import datetime

@dag(
    start_date=datetime(2024, 10, 1),
    schedule=None,  # No schedule, this DAG is triggered by another DAG
    catchup=False,
)
def transform_data():

    start_transformation = DummyOperator(task_id="start_transformation")

    finish_transformation = DummyOperator(task_id="finish_transformation")

    start_transformation >> finish_transformation

transform_data()