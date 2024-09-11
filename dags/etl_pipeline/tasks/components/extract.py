from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from helper.minio import MinioClient
from io import BytesIO
from datetime import timedelta

import pandas as pd
import json
import requests
import gspread

BASE_PATH = "/opt/airflow/dags"


class Extract:
    def _dellstore_db(table_name, incremental, **kwargs):
        """
        Extract data from Dellstore database.

        Args:
            table_name (str): Name of the table to extract data from.
            incremental (bool): Whether to extract incremental data or not.
            **kwargs: Additional keyword arguments.

        Raises:
            AirflowSkipException: If no new data is found.
        """
        pg_hook = PostgresHook(postgres_conn_id='dellstore_db')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()

        query = f"SELECT * FROM {table_name}"
        if incremental:
            date = kwargs['ds']
            query += f" WHERE created_at::DATE = '{date}'::DATE - INTERVAL '1 DAY';"

        cursor.execute(query)
        result = cursor.fetchall()
        column_list = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(result, columns=column_list)

        if df.empty:
            raise AirflowSkipException(f"{table_name} doesn't have new data. Skipped...")
        else:
            csv_bytes = df.to_csv(index=False).encode('utf-8')
            csv_buffer = BytesIO(csv_bytes)

            bucket_name = 'extracted-data'
            minio_client = MinioClient._get()
            object_name = (
                f'/temp/{table_name}-{(pd.to_datetime(date) - timedelta(days=1)).strftime("%Y-%m-%d")}.csv'
                if incremental
                else f'/temp/{table_name}.csv'
            )
            minio_client.put_object(
                bucket_name=bucket_name,
                object_name=object_name,
                data=csv_buffer,
                length=len(csv_bytes),
                content_type='application/csv'
            )

        cursor.close()
        connection.commit()
        connection.close()


    def _dellstore_api(ds):
        """
        Extract data from Dellstore API.

        Args:
            ds (str): Date string.

        Raises:
            AirflowSkipException: If no new data is found.
        """
        response = requests.get(
            url="https://api-history-order.vercel.app/api/dummydata",
            params={"start_date": ds, "end_date": ds},
        )

        if response.status_code == 200:
            json_data = response.json()

            if json_data:
                json_string = json.dumps(json_data)

                bucket_name = 'extracted-data'
                minio_client = MinioClient._get()
                object_name = f'/temp/dellstore_api_{(pd.to_datetime(ds) - timedelta(days=1)).strftime("%Y-%m-%d")}.json'
                json_bytes = json_string.encode('utf-8')
                minio_client.put_object(
                    bucket_name=bucket_name,
                    object_name=object_name,
                    data=BytesIO(json_bytes),
                    length=len(json_bytes),
                    content_type='application/json'
                )
            else:
                raise AirflowSkipException("No new data in Dellstore API. Skipped...")

    def _dellstore_spreadsheet():
        """
        Extract data from Dellstore spreadsheet.

        Raises:
            AirflowSkipException: If no data is found.
        """
        hook = GoogleBaseHook(gcp_conn_id="dellstore_analytics")
        credentials = hook.get_credentials()
        google_credentials = gspread.Client(auth=credentials)

        sheet = google_credentials.open("dellstore_analytic")
        worksheet = sheet.get_worksheet(0)

        df = pd.DataFrame(worksheet.get_all_records())

        if df.empty:
            raise AirflowSkipException("No data in Dellstore Analytics Spreadsheets. Skipped...")
        else:
            minio_client = MinioClient._get()
            bucket_name = 'extracted-data'
            csv_bytes = df.to_csv(index=False).encode('utf-8')
            csv_buffer = BytesIO(csv_bytes)

            minio_client.put_object(
                bucket_name=bucket_name,
                object_name=f'/temp/dellstore_analytics.csv',
                data=csv_buffer,
                length=len(csv_bytes),
                content_type='application/csv'
            )