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

DATE = '{{ ds }}'

@task_group
def warehouse(incremental):
    @task_group
    def step_1():
        @task_group
        def extract_transform():
            jar_list = [
                '/opt/spark/jars/hadoop-aws-3.3.1.jar',
                '/opt/spark/jars/aws-java-sdk-bundle-1.11.901.jar',
                '/opt/spark/jars/postgresql-42.2.23.jar'
            ]

            spark_conf = {
                'spark.hadoop.fs.s3a.access.key': 'minio',
                'spark.hadoop.fs.s3a.secret.key': 'minio123',
                'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
                'spark.hadoop.fs.s3a.path.style.access': 'true',
                'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem'
            }


            tables = ['categories', 'customers', 'products', 'inventory', 'orders', 'orderlines', 'cust_hist', 'order_status_analytic', 'orderlines_history']
            for table in tables:
                SparkSubmitOperator(
                    task_id=f'{table}',
                    conn_id='spark-conn',
                    application=f'dags/etl_pipeline/tasks/warehouse/components/extract_transform.py',
                    application_args=[
                        f'{table}',
                        f'{incremental}',
                        DATE
                    ],
                    conf=spark_conf,
                    jars=','.join(jar_list),
                    trigger_rule='none_failed',
                )
        extract_transform()
    step_1()