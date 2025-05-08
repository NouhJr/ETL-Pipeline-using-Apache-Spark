#Importing used packages
# import os
# from dotenv import load_dotenv
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from gcp_connection import load_bigquery_tables_to_df
from create_logs import write_log

#Loading secured variables from .env file
# load_dotenv()
# stg_dataset_id = os.environ.get('STG_DATASET_ID')

stg_dataframes = []
def start_delta(dataset_id, file_names):
    write_log("Strting delta phase....", "INFO")
    stg_dataframes = load_bigquery_tables_to_df(dataset_id, file_names)
    display_dataframes(stg_dataframes)


def display_dataframes(dfs):
    try:
        for df in dfs:
            df.show(5, truncate=False)
        write_log("Application finished successfully.", "INFO")
        exit()
    except Exception as e:
        write_log(f"{str(e)}", "ERROR")
        write_log(f"Application terminated.", "INFO")
        raise 

