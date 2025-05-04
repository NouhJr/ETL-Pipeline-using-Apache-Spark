#Importing used packages
import os
from dotenv import load_dotenv
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from create_logs import write_log

#Loading secured variables from .env file
load_dotenv()
stg_dataset_id = os.environ.get('STG_DATASET_ID')
service_account_file = os.environ.get('SERVICE_ACCOUNT_FILE')
project_id = os.environ.get('PROJECT_ID')

#Creating Apache Spark session.
spark = SparkSession.Builder().master("local")\
    .appName("ETL_Pipeline")\
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.28.0")\
        .config("parentProject", project_id)\
        .config("credentialsFile", service_account_file)\
        .config("spark.driver.extraJavaOptions", 
            "--add-opens=java.base/java.nio=ALL-UNNAMED "
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED") \
        .getOrCreate() # type: ignore


#Function to read data files into pyspark dataframe, takes the file path and format as parameters and return
# new dataframe.
def load_dataframe(filename, fileformat):
    df = spark.read.format(fileformat).options(header='true').load(filename)
    return df  

def load_bigquery_tables_to_df(gcp_project_id, data_set_id, list_files_names):
    write_log("Loading data from BigQuery....", "INFO")
    print(list_files_names)
    dataframes = []
    for source_file_name in list_files_names:
        table_name = source_file_name.lower()
        table = f"{data_set_id}.{table_name}"
        df = spark.read.format("bigquery").option("table", table).load()
        dataframes.append(df)
    write_log("Data loaded successfully.", "INFO")
    display_dataframes(dataframes)

def display_dataframes(dfs):
    for df in dfs:
        print(df)
    write_log("Application finished successfully.", "INFO")
    exit()    