from airflow.decorators import task_group
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from etl_pipeline.tasks.staging.components.extract import Extract
from etl_pipeline.tasks.staging.components.load import Load

@task_group
def dellstore_db(incremental):
    @task_group
    def extract():            
        table_to_extract = eval(Variable.get('list_dellstore_table'))

        for table_name in table_to_extract:
            current_task = PythonOperator(
                task_id = f'{table_name}',
                python_callable = Extract._dellstore_db,
                trigger_rule = 'none_failed',
                op_kwargs = {
                    'table_name': f'{table_name}',
                    'incremental': incremental
                }
            )

            current_task

    @task_group
    def load():
        table_to_load = eval(Variable.get('list_dellstore_table'))
        table_pkey = eval(Variable.get('pkey_dellstore_table'))
        previous_task = None
        
        for table_name in table_to_load:
            current_task = PythonOperator(
                task_id = f'{table_name}',
                python_callable = Load._dellstore_db,
                trigger_rule = 'none_failed',
                op_kwargs = {
                    'table_name': table_name,
                    'table_pkey': table_pkey,
                    'incremental': incremental
                },
            )

            if previous_task:
                previous_task >> current_task
    
            previous_task = current_task
            
    extract() >> load()