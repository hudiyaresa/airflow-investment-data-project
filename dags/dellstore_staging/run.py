from airflow.decorators import dag
from dellstore_staging.tasks.dellstore_db import dellstore_db
from dellstore_staging.tasks.dellstore_api import dellstore_api
from dellstore_staging.tasks.dellstore_spreadsheet import dellstore_spreadsheet
from datetime import datetime
from helper.callbacks.slack_notifier import slack_notifier
from airflow.models.variable import Variable

default_args = {
    "owner": "Rahil",
    "on_failure_callback": slack_notifier
}

@dag(
    dag_id="dellstore_staging",
    start_date=datetime(2024, 9, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["dellstore"],
    description="Extract, and Load Dellstore data into Staging Area"
)

def dellstore_staging_dag():
    incremental_mode = eval(Variable.get('dellstore_staging_incremental_mode'))
    # Run the dellstore_db task with the incremental flag
    db_task = dellstore_db(incremental=incremental_mode)
    
    # Run the dellstore_api task
    api_task = dellstore_api()
    
    # Run the dellstore_spreadsheet task
    spreadsheet_task = dellstore_spreadsheet()
    
    # Set the task dependencies
    [db_task, api_task, spreadsheet_task]

dellstore_staging_dag()