from airflow.providers.postgres.hooks.postgres import PostgresHook
from helper.minio import MinioClient, CustomMinio
from helper.postgres import Execute
from airflow.models import Variable
from io import BytesIO
import json

BASE_PATH = "/opt/airflow/dags"

class Extract:
    def _investment_db():

        df = Execute._get_dataframe(
            connection_id = 'investment_db',
            query_path = 'investment_data_profiling/query/get_profile_quality.sql'
        )

        df['data_profile'] = df['data_profile'].apply(json.dumps)
        df['data_quality'] = df['data_quality'].apply(json.dumps)

        CustomMinio._put_csv(
            dataframe=df,
            bucket_name='data-profile-quality',
            object_name='temp/investment_db_profiled.csv'
        )

    @staticmethod
    def _investment_api():
        url = Variable.get('investment_api_url')
        response = requests.get(url)
        response.raise_for_status()
        df = pd.DataFrame(response.json())

        profiled_df = profile_and_quality(df, table_name="milestone")

        CustomMinio._put_csv(
            dataframe=profiled_df,
            bucket_name='data-profile-quality',
            object_name='temp/api_milestone_data_profiled.csv'
        )

    @staticmethod
    def _local_csv():
        files = [
            {"file": "dim_date.csv", "table": "dim_date"},
            {"file": "people.csv", "table": "people"},
            {"file": "relationships.csv", "table": "relationships"}
        ]

        for item in files:
            file_path = os.path.join(BASE_PATH, 'investment_db', 'external', item['file'])
            df = pd.read_csv(file_path)

            profiled_df = profile_and_quality(df, table_name=item['table'])

            CustomMinio._put_csv(
                dataframe=profiled_df,
                bucket_name='data-profile-quality',
                object_name=f'temp/{item["table"]}_profiled.csv'
            )