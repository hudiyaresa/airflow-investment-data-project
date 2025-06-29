import os
from dotenv import load_dotenv
import pandas as pd

# Load environment variables
load_dotenv()

def get_jdbc_url(db_type):
    if db_type == 'source':
        return f"jdbc:postgresql://investment_db:5432/{os.getenv('INVESTMENT_DB_NAME')}"
    elif db_type == 'warehouse':
        return f"jdbc:postgresql://warehouse_db:5432/{os.getenv('WAREHOUSE_DB_NAME')}"

# Logging
def etl_log(log_msg: dict):
    try:
        conn = get_db_connection('warehouse')
        df_log = pd.DataFrame([log_msg])
        df_log.to_sql(name="etl_log", con=conn, if_exists="append", schema="log", index=False)
    except Exception as e:
        print(f"Can't save your log message. Cause: {str(e)}")