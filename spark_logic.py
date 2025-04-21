#Importing used packages
import os
import shutil
from dotenv import load_dotenv
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from create_logs import write_log
from gcp_connection import upload_dataframes_to_bigquery

#Creating Apache Spark session.
spark = SparkSession.Builder().master("local").appName("ETL_Pipeline").getOrCreate() # type: ignore


#Loading secured variables from .env file
load_dotenv()
reference_date = os.environ.get('REFERENCE_DATE')
source_dir_path = os.environ.get('SOURCE_FILES_DIR')
arch_dir_path = os.environ.get('SOURCE_FILES_ARCHIVE_DIR')
stg_dataset_id = os.environ.get('STG_DATASET_ID')


#Function to read data files into pyspark dataframe, takes the file path and format as parameters and return
# new dataframe.
def load_dataframe(filename, fileformat):
    df = spark.read.format(fileformat).options(header='true').load(filename)
    return df

#Function to add two new columns to each dataframe and upload it to big query. 
def stagging_logic(dataframes, list_source_files_names):
    write_log("Processing source data....", "INFO")
    for df in dataframes:
        df_rowHash = df.withColumn("ROW_HASH", md5(concat_ws("||", *df.columns)))
        final_df = df_rowHash.withColumn("REFERENCE_DATE", to_date(lit(reference_date)))
        # final_df.printSchema()
        # final_df.limit(5).show(truncate=False)
        upload_dataframes_to_bigquery(stg_dataset_id,final_df,list_source_files_names)
        #write_df_to_file(final_df)
    archive_files()
    write_log("Stagging area finished successfully.", "INFO")


#Function to move the processed source files to the archive directory.
def archive_files():
    for file_name in os.listdir(source_dir_path):
        if not os.path.exists(arch_dir_path+'/'+reference_date+'/'+file_name):
            write_log("Moving source files to archive directory....", "INFO")
            shutil.move(source_dir_path+'/'+file_name, arch_dir_path+'/'+reference_date)
            write_log("Source files archived successfully.", "INFO")
        else:    
            write_log(f"Source file: {file_name}, already archived.", "INFO")
            write_log(f"Deleting Source file: {file_name}, from source files dis....", "INFO")
            os.remove(source_dir_path + '/' + file_name)
            write_log(f"File: {file_name} deleted successfully.", "INFO")
    #Starting DELTA phase.

    # write_log("Application finished successfully.", "INFO")
    # exit()