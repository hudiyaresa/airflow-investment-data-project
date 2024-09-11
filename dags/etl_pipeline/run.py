from airflow.decorators import dag
from pendulum import datetime

from etl_pipeline.tasks.dellstore_db import dellstore_db
from etl_pipeline.tasks.dellstore_api import dellstore_api
from etl_pipeline.tasks.dellstore_spreadsheet import dellstore_spreadsheet

@dag(
    dag_id = 'etl_pipeline',
    description = 'ETL pipeline for extracting data from Dellstore database, API, and spreadsheet into staging area.',
    start_date = datetime(2024, 9, 1),
    schedule = "@daily",
    catchup = False
)

def etl_pipeline():

    dellstore_db() >> dellstore_api() >> dellstore_spreadsheet() 

etl_pipeline()