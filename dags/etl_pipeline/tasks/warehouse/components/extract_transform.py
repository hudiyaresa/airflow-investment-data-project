from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException, AirflowException
from airflow.models import Variable, TaskInstance

from etl_pipeline.tasks.warehouse.components.extract import _extract
from etl_pipeline.tasks.warehouse.components.validations import Validation, ValidationType
from helper.minio import CustomMinio

import re
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
import sys
from io import BytesIO
import tempfile
import os

class ExtractTransform:
    @staticmethod
    def _categories(incremental, date):
        try:
            spark = SparkSession.builder \
                .appName("ETL Pipeline - Categories") \
                .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
                .config("spark.hadoop.fs.s3a.access.key", "minio") \
                .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
                .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .getOrCreate()

            if incremental:
                column_list, data = _extract(connection_id='staging_db', table_name='staging.categories', incremental=incremental, date=date)
                object_name = f'categories-{date}.csv'
            else:
                column_list, data = _extract(connection_id='staging_db', table_name='staging.categories', incremental=incremental)
                object_name = f'categories.csv'

            if not data:
                spark.stop()
                print("categories doesn't have new data. Skipped...")
                return
            else:
                print(column_list)
                print(data)
                df = spark.createDataFrame(data, schema=column_list)
                df = df.withColumnRenamed('category', 'category_nk') \
                       .withColumnRenamed('categoryname', 'category_name') \
                       .dropDuplicates(['category_nk']) \
                       .drop('created_at')
                
                # Debug: Print DataFrame content
                print("DataFrame content before writing to CSV:")
                df.show(5)
                print('DF COUNT', df.count())


                df.write \
                .format("csv") \
                .option("header", "true") \
                .mode("overwrite") \
                .save("s3a://transformed-data/categories")
            
            spark.stop()

        except Exception as e:
            raise AirflowException(f"Error when extracting and transforming data from categories: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: extract_transform.py <function_name> <incremental> <date>")
        sys.exit(-1)

    function_name = sys.argv[1]
    incremental = sys.argv[2].lower() == 'true'
    date = sys.argv[3]

    if function_name == "categories":
        ExtractTransform._categories(incremental, date)
    else:
        print(f"Unknown function name: {function_name}")
        sys.exit(-1)