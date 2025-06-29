from airflow.decorators import task_group
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime
import pytz

# Constants
DATE = '{{ ds }}'

# Define the list of JAR files required for Spark
jar_list = [
    '/opt/spark/jars/hadoop-aws-3.3.1.jar',
    '/opt/spark/jars/aws-java-sdk-bundle-1.11.901.jar',
    '/opt/spark/jars/postgresql-42.2.23.jar'
]

# Define Spark configuration
spark_conf = {
    'spark.hadoop.fs.s3a.access.key': 'minio',
    'spark.hadoop.fs.s3a.secret.key': 'minio123',
    'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
    'spark.hadoop.fs.s3a.path.style.access': 'true',
    'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    'spark.dynamicAllocation.enabled': 'true',
    'spark.dynamicAllocation.maxExecutors': '3',
    'spark.dynamicAllocation.minExecutors': '1',
    'spark.dynamicAllocation.initialExecutors': '1',
    'spark.executor.memory': '4g',  # Define RAM per executor
    'spark.executor.cores': '2',  # Define cores per executor
    'spark.scheduler.mode': 'FAIR'
}

@task_group
def step_1(incremental):
    """
    Step 1 of the ETL process: Extract, Transform, Validate, and Load data for categories, customers, and customers_history.
    """

    @task_group
    def extract_transform():
        """
        Extract and transform data for categories, customers, and customers_history.
        """

        tables_and_dependencies = {
            'categories': "dellstore_db.load.categories",
            'customers': "dellstore_db.load.customers",
            'customers_history': "dellstore_api.load"
        }
        
        # Define a function to specify the execution date to monitor
        def target_execution_date(execution_date, **context):
            # Convert the naive datetime to a timezone-aware datetime
            return datetime(2004, 2, 28, tzinfo=pytz.UTC)
        
        for table_name, dependency in tables_and_dependencies.items():
            wait_for_staging = ExternalTaskSensor(
                task_id=f'wait_for_staging_{table_name}',
                external_dag_id='dellstore_staging',
                external_task_id=dependency,
                poke_interval=10,
                execution_date_fn=target_execution_date if table_name == 'customers_history' else None
            )
            
            spark_job = SparkSubmitOperator(
                task_id=f'{table_name}',
                conn_id='spark-conn',
                application=f'dags/dellstore_warehouse/tasks/components/extract_transform.py',
                application_args=[
                    f'{table_name}',
                    f'{incremental}',
                    DATE
                ],
                conf=spark_conf,
                jars=','.join(jar_list),
                trigger_rule='none_failed',
            )

            wait_for_staging >> spark_job

    @task_group
    def load():
        """
        Load data for categories, customers, and customers_history into the warehouse.
        """
        load_tasks = [
            ('categories', ['category_nk']),
            ('customers', ['customer_nk']),
            ('customers_history', ['customer_nk'])
        ]
        for table, table_pkey in load_tasks:
            SparkSubmitOperator(
                task_id=f'{table}',
                conn_id='spark-conn',
                application=f'dags/dellstore_warehouse/tasks/components/load.py',
                application_args=[
                    f'{table}',
                    ','.join(table_pkey),
                    f'{incremental}',
                    DATE
                ],
                conf=spark_conf,
                jars=','.join(jar_list),
                trigger_rule='none_failed',
            )

    # Define the order of execution for step 1
    extract_transform() >> load()

@task_group
def step_2(incremental):
    """
    Step 2 of the ETL process: Extract, Transform, Validate, and Load data for products and orders.
    """

    @task_group
    def extract_transform():
        """
        Extract and transform data for products and orders.
        """
        tables_and_dependencies = {
            'products_history': "dellstore_api.load",
            'orders_history': "dellstore_api.load",
            'products': "dellstore_db.load.products",
            'orders': "dellstore_db.load.orders"
        }

        def target_execution_date(execution_date, **context):
            return datetime(2004, 2, 28, tzinfo=pytz.UTC)

        for table_name, dependency in tables_and_dependencies.items():
            wait_for_staging = ExternalTaskSensor(
                task_id=f'wait_for_staging_{table_name}',
                external_dag_id='dellstore_staging',
                external_task_id=dependency,
                poke_interval=10,
                execution_date_fn=target_execution_date if table_name == 'orders_history' or table_name == 'products_history' else None
            )

            spark_job = SparkSubmitOperator(
                task_id=f'{table_name}',
                conn_id='spark-conn',
                application=f'dags/dellstore_warehouse/tasks/components/extract_transform.py',
                application_args=[
                    f'{table_name}',
                    f'{incremental}',
                    DATE
                ],
                conf=spark_conf,
                jars=','.join(jar_list),
                trigger_rule='none_failed',
            )

            wait_for_staging >> spark_job

    @task_group
    def load():
        """
        Load data for products and orders into the warehouse.
        """
        load_tasks = [
            ('products_history', ['product_nk']),
            ('orders_history', ['order_nk']),
            ('products', ['product_nk']),
            ('orders', ['order_nk'])
        ]
        for table, table_pkey in load_tasks:
            SparkSubmitOperator(
                task_id=f'{table}',
                conn_id='spark-conn',
                application=f'dags/dellstore_warehouse/tasks/components/load.py',
                application_args=[
                    f'{table}',
                    ','.join(table_pkey),
                    f'{incremental}',
                    DATE
                ],
                conf=spark_conf,
                jars=','.join(jar_list),
                trigger_rule='none_failed',
            )

    # Define the order of execution for step 2
    extract_transform() >> load()