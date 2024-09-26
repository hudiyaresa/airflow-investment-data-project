from airflow.decorators import task_group

from etl_pipeline.tasks.staging.dellstore_db import dellstore_db
from etl_pipeline.tasks.staging.dellstore_api import dellstore_api
from etl_pipeline.tasks.staging.dellstore_spreadsheet import dellstore_spreadsheet

@task_group
def staging(incremental):
    db_task = dellstore_db(incremental)
    api_task = dellstore_api()
    spreadsheet_task = dellstore_spreadsheet()

    db_task >> api_task >> spreadsheet_task