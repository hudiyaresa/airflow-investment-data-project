from airflow.decorators import dag
from datetime import datetime
from helper.callbacks.slack_notifier import slack_notifier
from airflow.models.variable import Variable
from dellstore_warehouse.tasks.main import (
    step_1,
    step_2,
    step_3
)

default_args = {
    "owner": "Rahil",
    "on_failure_callback": slack_notifier
}

@dag(
    start_date=datetime(2024, 9, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["dellstore"],
    description="Extract, Transform and Load Dellstore data into Warehouse"
)

def dellstore_warehouse():
    incremental_mode = eval(Variable.get('dellstore_warehouse_incremental_mode'))
    step_1(incremental=incremental_mode) >> step_2(incremental=incremental_mode) >> step_3(incremental=incremental_mode)

dellstore_warehouse()