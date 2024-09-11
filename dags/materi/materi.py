from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from datetime import datetime

# Define a simple function to use as the task
def process_data(dataset_id):
    print(f"Processing datasets {dataset_id}")
    # Add your data processing logic here

# Define a list of datasets to process
datasets = ["dataset_1", "dataset_2", "dataset_3"]

# Define the DAG
with DAG(
    'dynamic_tasks_examples',
    default_args={'owner': 'airflow', 'retries': 1},
    description='A simple example of dynamic tasks in Airflow',
    schedule_interval=None,
    start_date=datetime(2023, 9, 9),
    catchup=False,
) as dag:

    # Dynamically create tasks for each dataset
    for dataset in datasets:
        task = PythonOperator(
            task_id = f'process_{dataset}',
            python_callable = process_data,
            op_args = [dataset],
            dag = dag,
        )