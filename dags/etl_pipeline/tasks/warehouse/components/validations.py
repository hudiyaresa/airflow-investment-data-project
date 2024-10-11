import re
from datetime import timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import BooleanType
from functools import reduce

import sys
import pandas as pd

class ValidationType:
    """
    A class containing various static methods for data validation.
    """
    
    @staticmethod
    def validate_email_format(email):
        """
        Validate email format.
        """
        email_regex = re.compile(r"^[\w\.-]+@(yahoo\.com|hotmail\.com|gmail\.com)$")
        return bool(email_regex.match(email))
    
    @staticmethod
    def validate_phone_format(phone):
        """
        Validate phone number format.
        """
        phone = str(phone)
        phone_regex = re.compile(r"^\d{10}$")
        return bool(phone_regex.match(phone))
    
    @staticmethod
    def validate_credit_card_expiration_format(expiration_date):
        """
        Validate credit card expiration date format.
        """
        expiration_date_regex = re.compile(r"^\d{4}/\d{2}$")
        return bool(expiration_date_regex.match(expiration_date))   
    
    @staticmethod
    def validate_price_range(price):
        """
        Validate if the price is within the range 0 to 100.
        """
        return 0 <= price <= 100
    
    @staticmethod
    def validate_positive_value(value):
        """
        Validate if the value is positive.
        """
        return value >= 0
    
    @staticmethod
    def validate_order_status(status):
        """
        Validate if the order status is one of the allowed values.
        """
        return status in ['partial', 'fulfilled', 'backordered']

class Validation:
    """
    A class used to perform data validations.
    """
    
    @staticmethod
    def _data_validations(
        need_validation,
        table_name, 
        valid_bucket,
        invalid_bucket,  
        columns_to_validate,
        incremental, 
        date
    ):
        """
        Perform data validations on the specified table.

        Args:
            need_validation (bool): Flag to indicate if validation is needed.
            table_name (str): The name of the table to validate.
            valid_bucket (str): The S3 bucket for valid data.
            invalid_bucket (str): The S3 bucket for invalid data.
            columns_to_validate (str): The columns to validate.
            incremental (bool): Flag to indicate if the process is incremental.
            date (str): The date for incremental validation.
        """
        
        # Map validation function names to their corresponding methods
        function_map = {
            'validate_email_format': ValidationType.validate_email_format,
            'validate_phone_format': ValidationType.validate_phone_format,
            'validate_credit_card_expiration_format': ValidationType.validate_credit_card_expiration_format,
            'validate_price_range': ValidationType.validate_price_range,
            'validate_positive_value': ValidationType.validate_positive_value,
            'validate_order_status': ValidationType.validate_order_status
        }
        
        used_function = {}

        # Initialize Spark session
        spark = SparkSession.builder \
            .appName(f"Validate {table_name}") \
            .getOrCreate()
        
        try:
            # Define object name based on whether the process is incremental
            if incremental:
                object_name = f'{table_name}-{(pd.to_datetime(date) - timedelta(days=1)).strftime("%Y-%m-%d")}'
            else:
                object_name = f'{table_name}'
            
            # Read data from S3
            data = spark.read.options(
                delimiter=";", 
                header=True,
                inferSchema=True
            ).csv(f"s3a://transformed-data/{object_name}")

        except Exception as e:
            spark.stop()
            print(f"Table {table_name} doesn't have new data to validate. Skipped... : {e}")
            return

        try:
            if need_validation:
                columns_to_validate = eval(columns_to_validate)
                
                # Map columns to their corresponding validation functions
                for column, validation_type in columns_to_validate.items():
                    if validation_type in function_map and column in data.columns:
                        used_function[column] = function_map[validation_type]

                print("used_function")
                print(used_function)
                
                # Register UDFs for validation functions and compute 'all_valid' directly
                all_valid_conditions = []
                for name, func in used_function.items():
                    if name in data.columns:  # Check if column exists
                        udf_func = udf(func, BooleanType())
                        all_valid_conditions.append(udf_func(col(name)))

                print("all_valid_conditions")
                print(all_valid_conditions)

                # Ensure all conditions are combined into 'all_valid'
                if all_valid_conditions:
                    data = data.withColumn('all_valid', reduce(lambda a, b: a & b, all_valid_conditions))
                
                print("data after validation")
                print(data.show())
                
                # Filter out valid and invalid rows
                valid_data_df = data.filter(col('all_valid') == True)
                invalid_data_df = data.filter(col('all_valid') == False)

                # Drop 'all_valid' column before writing to MinIO
                valid_data_df = valid_data_df.drop('all_valid')
                invalid_data_df = invalid_data_df.drop('all_valid')

                # Write invalid data to S3 if any
                if invalid_data_df.count() > 0:
                    invalid_data_df.write \
                        .format("csv") \
                        .option("header", "true") \
                        .option("delimiter", ";") \
                        .mode("overwrite") \
                        .save(f"s3a://{invalid_bucket}/{object_name}")

                # Write valid data to S3
                valid_data_df.write \
                    .format("csv") \
                    .option("header", "true") \
                    .option("delimiter", ";") \
                    .mode("overwrite") \
                    .save(f"s3a://{valid_bucket}/{object_name}")
            else:
                # Write data to valid bucket if no validation is needed
                data.write \
                    .format("csv") \
                    .option("header", "true") \
                    .option("delimiter", ";") \
                    .mode("overwrite") \
                    .save(f"s3a://{valid_bucket}/{object_name}")
                
                spark.stop()
                print(f"{table_name} don't need to validate. Skipped...")
                return
        
        except Exception as e:
            spark.stop()
            raise Exception(f"Error when validating {table_name}: {e}")
        
if __name__ == "__main__":
    """
    Main entry point for the script.
    """
    if len(sys.argv) != 8:
        print("Usage: validations.py <table_name> <need_validation> <valid_bucket> <incremental> <invalid_bucket> <validation_type> <date>")
        sys.exit(-1)

    need_validation = sys.argv[1].lower() == 'true'
    table_name = sys.argv[2]
    valid_bucket = sys.argv[3]
    invalid_bucket = sys.argv[4]
    columns_to_validate = sys.argv[5]
    incremental = sys.argv[6].lower() == 'true'
    date = sys.argv[7]

    Validation._data_validations(
        need_validation,
        table_name, 
        valid_bucket,
        invalid_bucket,  
        columns_to_validate,
        incremental, 
        date
    )