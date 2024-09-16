from airflow.decorators import dag
from airflow.providers.slack.notifications.slack_notifier import SlackNotifier
from pendulum import datetime

from etl_pipeline.tasks.staging.dellstore_db import dellstore_db 
from etl_pipeline.tasks.staging.dellstore_api import dellstore_api
from etl_pipeline.tasks.staging.dellstore_spreadsheet import dellstore_spreadsheet

from etl_pipeline.tasks.warehouse.etl import warehouse

@dag(
    dag_id = 'etl_pipeline',
    description = 'ETL pipeline for extracting data from Dellstore database, API, and spreadsheet into staging area.',
    start_date = datetime(2024, 9, 1),
    schedule = "@daily",
    catchup = False,
    on_failure_callback = SlackNotifier(
        slack_conn_id = 'slack',
        channel = 'etlpipeline',
        text = 'ETL pipeline has failed!'
    )
)

def etl_pipeline():
    staging = dellstore_db(incremental = True) >> dellstore_api() >> dellstore_spreadsheet()

    staging >> warehouse(incremental = True)

etl_pipeline()