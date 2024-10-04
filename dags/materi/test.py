import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(
    dag_id="sparking_flow",
    default_args={
        "owner": "Yusuf Ganiyu",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval="@daily"
)

start = PythonOperator(
    task_id="start",
    python_callable=lambda: print("Jobs started"),
    dag=dag
)

wordcount_job = SparkSubmitOperator(
    task_id="wordcount_job",
    conn_id="spark-conn",
    application="jobs/wordcountjob.py",
    application_args=[
        "wordcountjob",  # Specify the function to run
        "Hello Spark Hello Python Hello Airflow Hello Docker and Hello Yusuf"
    ],
    dag=dag
)

character_count_job = SparkSubmitOperator(
    task_id="character_count_job",
    conn_id="spark-conn",
    application="jobs/wordcountjob.py",
    application_args=[
        "character_count_job",  # Specify the function to run
        "Hello Spark Hello Python Hello Airflow Hello Docker and Hello Yusuf"
    ],
    dag=dag
)

end = PythonOperator(
    task_id="end",
    python_callable=lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> wordcount_job >> character_count_job >> end