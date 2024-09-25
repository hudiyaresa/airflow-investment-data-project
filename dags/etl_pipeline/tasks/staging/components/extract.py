from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
from airflow import AirflowException
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from helper.minio import CustomMinio
from datetime import timedelta
from airflow.models import Variable

import pandas as pd
import requests
import gspread

BASE_PATH = "/opt/airflow/dags"


class Extract:
    @staticmethod
    def _dellstore_db(table_name, incremental, **kwargs):
        """
        Extract data from Dellstore database.

        Args:
            table_name (str): Name of the table to extract data from.
            incremental (bool): Whether to extract incremental data or not.
            **kwargs: Additional keyword arguments.

        Raises:
            AirflowException: If failed to extract data from Dellstore database.
            AirflowSkipException: If no new data is found.
        """
        try:
            pg_hook = PostgresHook(postgres_conn_id='dellstore_db')
            connection = pg_hook.get_conn()
            cursor = connection.cursor()

            query = f"SELECT * FROM {table_name}"
            if incremental:
                date = kwargs['ds']
                query += f" WHERE created_at::DATE = '{date}'::DATE - INTERVAL '1 DAY';"

                object_name = f'/temp/{table_name}-{(pd.to_datetime(date) - timedelta(days=1)).strftime("%Y-%m-%d")}.csv'
            
            else:
                object_name = f'/temp/{table_name}.csv'

            cursor.execute(query)
            result = cursor.fetchall()
            column_list = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(result, columns=column_list)

            if df.empty:
                raise AirflowSkipException(f"{table_name} doesn't have new data. Skipped...")

            bucket_name = 'extracted-data'
            CustomMinio._put_csv(df, bucket_name, object_name)

            cursor.close()
            connection.commit()
            connection.close()

        except Exception as e:
            raise AirflowException(f"Error when extracting {table_name} : {str(e)}")

    @staticmethod
    def _dellstore_api(ds):
        """
        Extract data from Dellstore API.

        Args:
            ds (str): Date string.

        Raises:
            AirflowException: If failed to fetch data from Dellstore API.
            AirflowSkipException: If no new data is found.
        """
        response = requests.get(
            url=Variable.get('dellstore_api_url'),
            params={"start_date": ds, "end_date": ds},
        )

        if response.status_code != 200:
            raise AirflowException(f"Failed to fetch data from Dellstore API. Status code: {response.status_code}")

        json_data = response.json()
        if not json_data:
            raise AirflowSkipException("No new data in Dellstore API. Skipped...")

        bucket_name = 'extracted-data'
        object_name = f'/temp/dellstore_api_{(pd.to_datetime(ds) - timedelta(days=1)).strftime("%Y-%m-%d")}.json'
        CustomMinio._put_json(json_data, bucket_name, object_name)

    @staticmethod
    def _dellstore_spreadsheet():
        """
        Extract data from Dellstore spreadsheet.

        Raises:
            AirflowSkipException: If no data is found.
            AirflowException: If failed to extract data from Dellstore spreadsheet.
        """
        try:
            hook = GoogleBaseHook(gcp_conn_id="dellstore_analytics")
            credentials = hook.get_credentials()
            google_credentials = gspread.Client(auth=credentials)

            sheet = google_credentials.open("dellstore_analytic")
            worksheet = sheet.get_worksheet(0)

            df = pd.DataFrame(worksheet.get_all_records())

            if df.empty:
                raise AirflowSkipException("No data in Dellstore Analytics Spreadsheets. Skipped...")

            CustomMinio._put_csv(df, 'extracted-data', f'/temp/dellstore_analytics.csv')

        except Exception as e:
            raise AirflowException(f"Error when extracting Dellstore Analytics Spreadsheets: {str(e)}")