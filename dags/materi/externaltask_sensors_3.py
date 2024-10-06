from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime

@dag(
    start_date=datetime(2023, 10, 1), 
    schedule='* * * * *', 
    catchup=False
)

def process_data_3():
    wait_for_extract = ExternalTaskSensor(
        task_id='wait_for_extract',
        external_dag_id='process_data_1',
        external_task_id='extract_data',
        poll_interval=5
    )
    transform_data_v3 = DummyOperator(task_id='transform_data_3')
    load_data_v3 = DummyOperator(task_id='load_data_3')

    # Set the dependencies
    wait_for_extract >> transform_data_v3 >> load_data_v3

process_data_3()