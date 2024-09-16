from airflow.decorators import task_group
from airflow.operators.python import PythonOperator

from etl_pipeline.tasks.staging.components.extract import Extract
from etl_pipeline.tasks.staging.components.load import Load

@task_group()
def dellstore_api():
    extract = PythonOperator(
        task_id = 'extract',
        python_callable = Extract._dellstore_api,
        trigger_rule = 'none_failed'
    )

    load = PythonOperator(
        task_id = 'load',
        python_callable = Load._dellstore_api
    )

    extract >> load