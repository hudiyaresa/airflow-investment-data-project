from airflow.operators.bash import BashOperator
from pendulum import datetime
from airflow.decorators import dag

@dag(
    dag_id = "date_generators",
    start_date = datetime(2024, 9, 1),
    schedule = "@daily",
    catchup = True,
)
def date_generator():
    generate_date = BashOperator(task_id = "generate_date",
                                 bash_command = "echo 'airflow' | sudo -S bash -c 'echo \"Logical Date: {{ ds }}\" >> /opt/airflow/dags/src_dir/date.txt'")
    
    copy_date_file = BashOperator(task_id = "copy_date_file",
                                  bash_command = "echo 'airflow' | sudo -S cp /opt/airflow/dags/src_dir/date.txt /opt/airflow/dags/dest_dir")
    
    generate_date >> copy_date_file

date_generator()