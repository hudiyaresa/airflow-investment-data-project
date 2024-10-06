from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

@dag(
    start_date=datetime(2024, 10, 1),
    schedule_interval="@daily",
    catchup=False,
)
def ingest_data():
    start_ingestion = DummyOperator(task_id="start_ingestion")

    finish_ingestion = DummyOperator(task_id="finish_ingestion")

    trigger_transformation = TriggerDagRunOperator(
        task_id="trigger_transform_dag",
        trigger_dag_id="transform_data",  # ID of the DAG to trigger
    )

    start_ingestion >> finish_ingestion >> trigger_transformation

ingest_data()