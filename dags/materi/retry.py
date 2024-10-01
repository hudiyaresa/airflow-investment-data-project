from airflow.decorators import dag, task
from datetime import datetime
import time
from datetime import timedelta

default_args={
    'retries': 2,  # Number of retries
    'retry_delay': timedelta(seconds = 5),  # Delay between retries
}

@dag(
    dag_id = "retry_dag",
    start_date = datetime.now(),
    schedule="@daily",
    default_args = default_args
)
def retry_dag():
    @task
    def sleep_task():
        time.sleep(5)
        raise Exception("Task Failed")
    
    # Tasks that have different number of retries and retry delay
    @task(retries=3, retry_delay = timedelta(seconds = 5))
    def sleep_task_2():
        time.sleep(5)
        raise Exception("Task Failed")

    sleep_task()
    sleep_task_2()

retry_dag()