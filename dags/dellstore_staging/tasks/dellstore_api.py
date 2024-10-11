from airflow.decorators import task_group
from airflow.operators.python import PythonOperator

from dellstore_staging.tasks.components.extract import Extract
from dellstore_staging.tasks.components.load import Load

@task_group()
def dellstore_api():
    """
    Task group for Dellstore API Extract and Load process.
    """

    # Define the extract task
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=Extract._dellstore_api,
        trigger_rule='none_failed'
    )

    # Define the load task
    load_task = PythonOperator(
        task_id='load',
        python_callable=Load._dellstore_api,
        trigger_rule='none_failed'
    )

    # Set task dependencies
    extract_task >> load_task