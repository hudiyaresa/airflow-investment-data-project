from airflow import DAG
from airflow.decorators import task
from datetime import datetime

# Define the DAG
with DAG(
    'dynamic_tasks_with_expand_examples',
    default_args={'owner': 'airflow', 'retries': 1},
    description='A simple example of dynamic tasks using expand() in Airflow',
    schedule_interval=None,
    start_date=datetime(2023, 9, 9),
    catchup=False,
) as dag:

    # Define a list of datasets to process
    datasets = ["dataset_1", "dataset_2", "dataset_3"]

    @task
    def process_data(dataset_id):
        print(f"Processing dataset {dataset_id}")
        # Add your data processing logic here

    # Create dynamic tasks using expand
    process_data.expand(dataset_id=datasets)
