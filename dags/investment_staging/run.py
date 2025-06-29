from airflow.decorators import dag
from investment_staging.tasks.investment_db import investment_db
from investment_staging.tasks.investment_api import investment_api
from datetime import datetime
from helper.callbacks.slack_notifier import slack_notifier
from airflow.models.variable import Variable

default_args = {
    "owner": "Resa",
    "on_failure_callback": slack_notifier
}

@dag(
    dag_id="investment_staging",
    start_date=datetime(2024, 9, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["Startup", "Investment"],
    description="Extract, and Load Investment data into Staging Area"
)

def investment_staging_dag():
    incremental_mode = eval(Variable.get('investment_staging_incremental_mode'))
    # Run the investment_db task with the incremental flag
    db_task = investment_db(incremental=incremental_mode)
    
    # Run the investment_api task
    api_task = investment_api()
        
    # Set the task dependencies
    [db_task, api_task]

investment_staging_dag()