from helper.minio import MinioClient
from helper.postgres import Execute
import pandas as pd

BASE_PATH = "/opt/airflow/dags"

class TransformLoad:
    def _dellstore_db():
        minio_client = MinioClient._get()
        bucket_name = 'data-profile-quality'
        object_name = f"/temp/dellstore_db.csv"

        data = minio_client.get_object(
            bucket_name = bucket_name,
            object_name = object_name
        )

        df = pd.read_csv(data)

        df.insert(
            loc = 0, 
            column = "person_in_charge", 
            value = "Rahil"
        )
        df.insert(
            loc = 1, 
            column = "source", 
            value = "dellstore_db"
        )

        Execute._insert_dataframe(
                connection_id = "profile_quality_db", 
                query_path = "/profiling_quality_pipeline/query/insert_profile_quality.sql",
                dataframe = df
        )