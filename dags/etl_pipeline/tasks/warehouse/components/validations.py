import re
import pandas as pd
from helper.minio import CustomMinio
from airflow.exceptions import AirflowSkipException

class ValidationType:
    #validation email domain
    def validate_email_format(email):
        email_regex = re.compile(r"^[\w\.-]+@(yahoo\.com|hotmail\.com|gmail\.com)$")
        return bool(email_regex.match(email))
    
    # Ensure phone number contains 10 digits
    def validate_phone_format(phone):
        phone = str(phone)
        phone_regex = re.compile(r"^\d{10}$")
        return bool(phone_regex.match(phone))
    
    # Validate credit card number expiration date format is YYYY/MM
    def validate_credit_card_expiration_format(expiration_date):
        expiration_date_regex = re.compile(r"^\d{4}/\d{2}$")
        return bool(expiration_date_regex.match(expiration_date))   
    
    # Ensure that the price value is within the range of 0 to 100.
    def validate_price_range(price):
        return 0 <= price <= 100
    
    # Ensure that net_amount, tax, and total_amount are positive values.
    def validate_positive_value(value):
        return value >= 0
    
    #  Validate that the status is either partial, fulfilled, or backordered.
    def validate_order_status(status):
        return status in ['partial', 'fulfilled', 'backordered']
    
class Validation:
    def _validation_data(need_validation, data, valid_bucket, dest_object, invalid_bucket = None, validation_functions = None ):
        data = CustomMinio._get_dataframe('transformed-data', data)

        print(data)
        print(data.info())
        if need_validation:
            # Create a report DataFrame
            report_data = {f'validate_{name}': data[name].apply(func) for name, func in validation_functions.items()}
            report_df = pd.DataFrame(report_data)

            # Summarize status data by all conditions
            report_df['all_valid'] = report_df.all(axis = 1)

            # Filter out valid rows (all_valid = 'True')
            valid_data_df = data[report_df['all_valid']]

            # Filter out invalid rows (all_valid = 'False')
            invalid_data_df = data[~report_df['all_valid']]

            if (not invalid_data_df.empty):
                CustomMinio._put_csv(invalid_data_df, invalid_bucket, dest_object)

            else:
                CustomMinio._put_csv(valid_data_df, valid_bucket, dest_object)

        else:
            CustomMinio._put_csv(data, valid_bucket, dest_object)
            raise AirflowSkipException(f"{data} don't need to validate. Skipped...")