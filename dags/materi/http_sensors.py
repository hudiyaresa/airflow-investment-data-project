from airflow.decorators import dag
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.dummy import DummyOperator
from datetime import datetime

@dag(
    start_date=datetime(2024, 9, 1), 
    schedule_interval='@daily', 
    catchup=False
)

def http_sensor():
    wait_for_api = HttpSensor(
        task_id='wait_for_api_data',
        http_conn_id='dummy_api',
        endpoint='/api/movies',
        method='GET',
        poke_interval=60 * 5,  # Check every 5 minutes
        timeout=60 * 60 * 24  # Wait for up to 24 hours
    )

    extract_api_data = DummyOperator(task_id='extract_api_data')

    wait_for_api >> extract_api_data

http_sensor()