import re
from helper.minio import CustomMinio
from airflow.exceptions import AirflowSkipException, AirflowException
from datetime import timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import BooleanType

class ValidationType:
    @staticmethod
    def validate_email_format(email):
        email_regex = re.compile(r"^[\w\.-]+@(yahoo\.com|hotmail\.com|gmail\.com)$")
        return bool(email_regex.match(email))
    
    @staticmethod
    def validate_phone_format(phone):
        phone = str(phone)
        phone_regex = re.compile(r"^\d{10}$")
        return bool(phone_regex.match(phone))
    
    @staticmethod
    def validate_credit_card_expiration_format(expiration_date):
        expiration_date_regex = re.compile(r"^\d{4}/\d{2}$")
        return bool(expiration_date_regex.match(expiration_date))   
    
    @staticmethod
    def validate_price_range(price):
        return 0 <= price <= 100
    
    @staticmethod
    def validate_positive_value(value):
        return value >= 0
    
    @staticmethod
    def validate_order_status(status):
        return status in ['partial', 'fulfilled', 'backordered']
    
class Validation:
    def _data_validations(table_name, need_validation, valid_bucket, incremental, invalid_bucket = None, validation_functions = None, date = None, **kwargs):
        try:
            if incremental:
                object_name = f'{table_name}-{date}.csv'
            else:
                object_name = f'{table_name}.csv'
            
            data = CustomMinio._get_spark_dataframe('transformed-data', object_name)
        except Exception as e:
            raise AirflowSkipException(f"Table {table_name} doesn't have new data to validate. Skipped... : {e}")

        try:
            if need_validation:
                for name, func in validation_functions.items():
                    udf_func = udf(func, BooleanType())
                    data = data.withColumn(f'validate_{name}', udf_func(col(name)))

                valid_data_df = data.filter(" AND ".join([f"validate_{name}" for name in validation_functions.keys()]))
                invalid_data_df = data.filter(" OR ".join([f"NOT validate_{name}" for name in validation_functions.keys()]))

                if invalid_data_df.count() > 0:
                    CustomMinio._put_csv(invalid_data_df, invalid_bucket, object_name)
                else:
                    CustomMinio._put_csv(valid_data_df, valid_bucket, object_name)
            else:
                CustomMinio._put_csv(data, valid_bucket, object_name)
                raise AirflowSkipException(f"{table_name} don't need to validate. Skipped...")
        except AirflowSkipException as e:
            raise e
        
        except Exception as e:
            raise Exception(f"Error when validating {table_name}: {e}")