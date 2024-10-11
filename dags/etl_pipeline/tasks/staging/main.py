from airflow.decorators import task_group

from etl_pipeline.tasks.staging.dellstore_db import dellstore_db
from etl_pipeline.tasks.staging.dellstore_api import dellstore_api
from etl_pipeline.tasks.staging.dellstore_spreadsheet import dellstore_spreadsheet

@task_group
def staging(incremental):
    """
    Task group for staging tasks in the ETL pipeline.

    Args:
        incremental (bool): Flag to indicate if the task should run in incremental mode.
    """
    
    # Run the dellstore_db task with the incremental flag
    db_task = dellstore_db(incremental=incremental)
    
    # Run the dellstore_api task
    api_task = dellstore_api()
    
    # Run the dellstore_spreadsheet task
    spreadsheet_task = dellstore_spreadsheet()
    
    # Set the task dependencies
    [db_task, api_task, spreadsheet_task]