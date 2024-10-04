from airflow.hooks.base import BaseHook
from minio import Minio
from io import BytesIO
import json
import pandas as pd
import os

class MinioClient:
    @staticmethod
    def _get():
        minio = BaseHook.get_connection('minio')
        client = Minio(
            endpoint = minio.extra_dejson['endpoint_url'],
            access_key = minio.login,
            secret_key = minio.password,
            secure = False
        )

        return client
    
class CustomMinio:
    @staticmethod
    def _put_csv_from_buffer(buffer, bucket_name, object_name):
        minio_client = MinioClient._get()
        minio_client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=buffer,
            length=buffer.getbuffer().nbytes,
            content_type='application/csv'
        )

    @staticmethod
    def _get_dataframe(bucket_name, object_name):
        minio_client = MinioClient._get()
        response = minio_client.get_object(bucket_name, object_name)
        csv_data = response.read()
        response.close()
        response.release_conn()
        
        if not csv_data.strip():
            raise ValueError("The CSV file is empty or only contains whitespace.")
        
        df = pd.read_csv(BytesIO(csv_data))
        return df
    
    @staticmethod
    def _put_csv_from_dataframe(dataframe, bucket_name, object_name):
        csv_bytes = dataframe.to_csv(index=False).encode('utf-8')
        csv_buffer = BytesIO(csv_bytes)

        minio_client = MinioClient._get()
        minio_client.put_object(
            bucket_name = bucket_name,
            object_name = object_name,
            data = csv_buffer,
            length = len(csv_bytes),
            content_type = 'application/csv'
        )

    @staticmethod
    def _put_csv_from_file(file_path, bucket_name, object_name):
        with open(file_path, 'rb') as file:
            file_data = file.read()

        minio_client = MinioClient._get()
        minio_client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=BytesIO(file_data),  # Use BytesIO to handle file data as a stream
            length=len(file_data),
            content_type='application/csv'
        )

    @staticmethod
    def _put_json(json_data, bucket_name, object_name):
        json_string = json.dumps(json_data)
        json_bytes = json_string.encode('utf-8')
        json_buffer = BytesIO(json_bytes)

        minio_client = MinioClient._get()
        minio_client.put_object(
            bucket_name = bucket_name,
            object_name = object_name,
            data = json_buffer,
            length = len(json_bytes),
            content_type = 'application/json'
        )

    @staticmethod
    def _get_dataframe(bucket_name, object_name):
        minio_client = MinioClient._get()
        data = minio_client.get_object(
            bucket_name = bucket_name,
            object_name = object_name
        )

        df = pd.read_csv(data)

        return df 