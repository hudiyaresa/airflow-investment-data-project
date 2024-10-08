from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from time import sleep

# Define the DAG
@dag(
    start_date=datetime(2023, 10, 1), 
    schedule='* * * * *', 
    catchup=False
)

def process_data_1():
    @task
    def extract_data():
        print("Extract data")
        sleep(30)

    transform_data_v1 = DummyOperator(task_id='transform_data_v1')
    load_data_v1 = DummyOperator(task_id='load_data_v1')

    # Set the dependencies
    extract_data() >> transform_data_v1 >> load_data_v1

process_data_1()