from airflow.decorators import dag
from pendulum import datetime

from etl_pipeline.tasks.staging.main import staging
from etl_pipeline.tasks.warehouse.main import warehouse
from helper.callbacks.slack_notifier import slack_notifier
from airflow.models.variable import Variable

default_args = {
    'on_failure_callback': slack_notifier
}

@dag(
    dag_id='etl_pipeline',
    description='ETL pipeline for extracting and loading data from Dellstore database, API, and spreadsheet into the staging area and then into the data warehouse.',
    start_date=datetime(2024, 9, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args
)

def etl_pipeline():
    incremental_mode = eval(Variable.get('etl_pipeline_incremental_mode'))

    staging(incremental=incremental_mode) 

etl_pipeline()