from airflow.decorators import dag
from airflow.providers.slack.notifications.slack_notifier import SlackNotifier
from pendulum import datetime
import traceback

from etl_pipeline.tasks.staging.main import staging
from etl_pipeline.tasks.warehouse.main import warehouse

@dag(
    dag_id='etl_pipeline',
    description='ETL pipeline for extracting and loading data from Dellstore database, API, and spreadsheet into the staging area and then into the data warehouse.',
    start_date=datetime(2024, 9, 1),
    schedule="@daily",
    catchup=False,
    on_failure_callback=SlackNotifier(
        slack_conn_id='slack',
        channel='etlpipeline',
        text="ETL Pipeline failed"
    )
)

def etl_pipeline():
    staging(incremental=True) >> warehouse(incremental=True)

etl_pipeline()