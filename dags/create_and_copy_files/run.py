from airflow.operators.bash import BashOperator
from pendulum import datetime
from airflow.decorators import dag

@dag(
    dag_id = "create_and_copy_files",
    start_date = datetime(2024, 9, 1),
    schedule = "@once",
    catchup = False,
)
def create_and_copy_files():
    create_src_dir = BashOperator(task_id = "create_src_dir",
                                  bash_command = "echo 'airflow' | sudo -S mkdir -p /opt/airflow/dags/src_dir")
    
    create_dest_dir = BashOperator(task_id = "create_dest_dir",
                                  bash_command = "echo 'airflow' | sudo -S mkdir -p /opt/airflow/dags/dest_dir")

    create_file = BashOperator(task_id = "create_file",
                               bash_command = "echo 'airflow' | sudo -S touch /opt/airflow/dags/src_dir/date.txt")
    
    copy_file = BashOperator(task_id = "copy_file",
                             bash_command = "echo 'airflow' | sudo -S cp /opt/airflow/dags/src_dir/date.txt /opt/airflow/dags/dest_dir")
    
    create_src_dir >> create_dest_dir >> create_file >> copy_file

create_and_copy_files()