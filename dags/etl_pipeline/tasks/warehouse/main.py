from airflow.decorators import task_group
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowSkipException
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator

# from etl_pipeline.tasks.warehouse.components.extract_transform import ExtractTransform 
# from etl_pipeline.tasks.warehouse.components.validations import Validation, ValidationType
# from etl_pipeline.tasks.warehouse.components.load import Load

BASE_PATH = 'dags'
DATE = '{{ ds }}'

@task_group
def warehouse(incremental):
    @task_group
    def step_1():
        @task_group
        def extract_transform():
            SparkSubmitOperator(
                task_id='categories',
                application=f'dags/etl_pipeline/tasks/warehouse/components/extract_transform.py',
                trigger_rule='none_failed',
                application_args=[
                    'categories',
                    f'{incremental}',
                    DATE
                ],
                conn_id='spark-conn',
                jars='/opt/spark/jars/hadoop-aws-3.3.1.jar,/opt/spark/jars/aws-java-sdk-bundle-1.11.900.jar'
            )
        extract_transform()
    step_1()