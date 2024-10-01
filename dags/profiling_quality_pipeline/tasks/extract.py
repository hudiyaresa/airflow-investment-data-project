from airflow.providers.postgres.hooks.postgres import PostgresHook
from helper.minio import MinioClient
from helper.postgres import Execute
from io import BytesIO
import json

BASE_PATH = "/opt/airflow/dags"

class Extract:
    def _dellstore_db():

        df = Execute._get_dataframe(
            connection_id = 'dellstore_db',
            query_path = 'profiling_quality_pipeline/query/get_profile_quality.sql'
        )

        df['data_profile'] = df['data_profile'].apply(json.dumps)
        df['data_quality'] = df['data_quality'].apply(json.dumps)

        bucket_name = 'data-profile-quality'
        minio_client = MinioClient._get()
        
        csv_bytes = df.to_csv(index = False).encode('utf-8')
        csv_buffer = BytesIO(csv_bytes)

        minio_client.put_object(
            bucket_name = bucket_name,
            object_name = f'/temp/dellstore_db.csv',
            data = csv_buffer,
            length = len(csv_bytes),
            content_type='application/csv'
        )