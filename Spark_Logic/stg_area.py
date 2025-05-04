#Importing used packages
import os
import shutil
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from dotenv import load_dotenv
from create_logs import write_log
from gcp_connection import upload_dataframes_to_bigquery
from spark_initialize import load_dataframe

#Loading secured variables from .env file
load_dotenv()
reference_date = os.environ.get('REFERENCE_DATE')
source_dir_path = os.environ.get('SOURCE_FILES_DIR')
arch_dir_path = os.environ.get('SOURCE_FILES_ARCHIVE_DIR')
stg_dataset_id = os.environ.get('STG_DATASET_ID')

#Function to get file path and file format to use it to create new spark dataframe from this file.
list_dfs = []
list_source_files_names = []
def get_files_info(files_dict):
    try:
        for file_name in files_dict:
            list_dfs.append(load_dataframe(file_name, files_dict[file_name]))
            file_name_txt = os.path.basename(file_name)
            file = os.path.splitext(file_name_txt)
            list_source_files_names.append(file[0])
        # Calling the stagging logic function to process the dataframes and upload them to big query.  
        stagging_logic(list_dfs, list_source_files_names)
    except Exception as e:
        write_log(f"{str(e)}", "ERROR")    


file_names = []
#Function to add two new columns to each dataframe and upload it to big query.
def stagging_logic(dataframes, list_source_files_names):
    file_names = list_source_files_names
    try:
        write_log("Processing source data....", "INFO")
        for df, file in zip(dataframes, list_source_files_names):
            df_rowHash = df.withColumn("ROW_HASH", md5(concat_ws("||", *df.columns)))
            final_df = df_rowHash.withColumn("REFERENCE_DATE", to_date(lit(reference_date)))
            final_df.show(5,truncate=False)
            upload_dataframes_to_bigquery(stg_dataset_id,final_df,file,"stg")
        archive_files()
        write_log("Stagging area finished successfully.", "INFO")
        #Starting DELTA phase.
        # write_log("Strting delta phase....", "INFO")
        # load_bigquery_tables_to_df(project_id, stg_dataset_id, file_names)
    except Exception as e:
        write_log(f"{str(e)}", "ERROR")
        write_log(f"Application terminated.", "INFO")
        raise    

#Function to move the processed source files to the archive directory.
def archive_files():
    try:
        for file_name in os.listdir(source_dir_path):
            if not os.path.exists(arch_dir_path+'/'+reference_date+'/'+file_name):
                write_log("Moving source files to archive directory....", "INFO")
                shutil.move(source_dir_path+'/'+file_name, arch_dir_path+'/'+reference_date)
                write_log("Source files archived successfully.", "INFO")  
            else:    
                write_log(f"Source file: {file_name}, already archived.", "INFO")
                write_log(f"Deleting Source file: {file_name}, from source files dir....", "INFO")
                os.remove(source_dir_path + '/' + file_name)
                write_log(f"File: {file_name} deleted successfully.", "INFO")
    except Exception as e:
        write_log(f"{str(e)}", "ERROR")
        write_log(f"Application terminated.", "INFO")
        raise     
    write_log("Application finished successfully.", "INFO")
    exit()

    




