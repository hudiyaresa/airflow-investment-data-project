from helper.minio import MinioClient, CustomMinio
from helper.postgres import Execute
import pandas as pd

BASE_PATH = "/opt/airflow/dags"

class TransformLoad:
    def _investment_db():
        minio_client = MinioClient._get()
        bucket_name = 'data-profile-quality'
        object_name = f"/temp/investment_db_profiled.csv"

        data = minio_client.get_object(
            bucket_name = bucket_name,
            object_name = object_name
        )

        df = pd.read_csv(data)

        df.insert(
            loc = 0, 
            column = "person_in_charge", 
            value = "Resa"
        )
        df.insert(
            loc = 1, 
            column = "source", 
            value = "public(investment_db)"
        )

        Execute._insert_dataframe(
                connection_id = "warehouse_db", 
                query_path = "investment_data_profiling/query/insert_profile_quality.sql",
                dataframe = df
        )

    def _external_sources():
        sources = [
            {"object_name": "temp/api_milestone_data_profiled.csv", "person_in_charge": "Resa"},
            {"object_name": "temp/dim_date_profiled.csv", "person_in_charge": "Resa"},
            {"object_name": "temp/people_profiled.csv", "person_in_charge": "Resa"},
            {"object_name": "temp/relationships_profiled.csv", "person_in_charge": "Resa"}
        ]

        for source in sources:
            df = CustomMinio._get_dataframe(
                bucket_name='data-profile-quality',
                object_name=source["object_name"]
            )

            df.insert(0, "person_in_charge", source["person_in_charge"])
            df.insert(
                loc = 1, 
                column = "source", 
                value = "-"
            )            

            Execute._insert_dataframe(
                connection_id="warehouse_db",
                query_path="investment_data_profiling/query/insert_profile_quality.sql",
                dataframe=df
            )