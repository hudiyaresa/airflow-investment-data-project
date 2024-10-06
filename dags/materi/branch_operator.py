from datetime import datetime
from airflow.decorators import dag, task, task_group

@dag(
    dag_id='etl_branch',
    start_date=datetime(2024, 9, 1),
    schedule='@daily',
    catchup=False,
)

def etl_branch():
    @task.branch
    def check_availability():
        is_available = True
        if is_available:
            return 'process_data'
        else:
            return 'send_alert'

    @task_group
    def process_data():
        @task
        def extract():
            print("Extract data")
        @task
        def transform():
            print("Transform data")
        @task
        def load():
            print("Load data")

        extract() >> transform() >> load()

    @task    
    def send_alert():
        print(f"Send alert")

    check_availability() >> [process_data(), send_alert()]

etl_branch()