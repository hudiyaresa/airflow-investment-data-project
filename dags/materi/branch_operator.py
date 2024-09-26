from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator

def choose_branch(**kwargs):
    if 2 % 2 == 0:
        return 'even_branch'
    else:
        return 'odd_branch'

def print_even():
    print("This is an even second.")

def print_odd():
    print("This is an odd second.")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    'branch_python_operator_example',
    default_args=default_args,
    description='A simple branch python operator example',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    start = DummyOperator(
        task_id='start'
    )

    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=choose_branch,
    )

    even_branch = PythonOperator(
        task_id='even_branch',
        python_callable=print_even
    )

    odd_branch = PythonOperator(
        task_id='odd_branch',
        python_callable=print_odd
    )

    end = DummyOperator(
        task_id='end',
        trigger_rule='none_failed_or_skipped'
    )

    start >> branching
    branching >> even_branch >> end
    branching >> odd_branch >> end