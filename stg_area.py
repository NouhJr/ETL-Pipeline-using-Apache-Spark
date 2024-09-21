#Importing used packages
import os
import shutil
from dotenv import load_dotenv
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

#Creating Apache Spark session.
spark = SparkSession.Builder().master("local").appName("ETL_Pipeline").getOrCreate() # type: ignore
spark


#Loading secured variables from .env file
load_dotenv()
reference_date = os.environ.get('REFERENCE_DATE')
source_dir_path = os.environ.get('SOURCE_FILES_DIR')
arch_dir_path = os.environ.get('SOURCE_FILES_ARCHIVE_DIR')
stg_files_dir_path = os.environ.get('STG_FILES_DIR')

#Function to read data files into pyspark dataframe, takes the file path and format as parameters and return
# new dataframe.
def load_dataframe(filename, fileformat):
    df = spark.read.format(fileformat).options(header='true').load(filename)
    return df

#Function to get file path and file format to use it to create new dataframe from this file.
list_dfs = []
list_source_files_names = []
def get_files_info(files_dict):
    for file_name in files_dict:
        list_dfs.append(load_dataframe(file_name, files_dict[file_name]))
        file_name_txt = os.path.basename(file_name)
        file = os.path.splitext(file_name_txt)
        list_source_files_names.append(file[0])
    add_stg_columns(list_dfs)    

#Function to add two new columns to each dataframe.
def add_stg_columns(dataframes):
    for df in dataframes:
        df_rowHash = df.withColumn("ROW_HASH", md5(concat_ws("||", *df.columns)))
        final_df = df_rowHash.withColumn("REFERENCE_DATE", lit(reference_date))
        #final_df.limit(5).show(truncate=False)
        write_df_to_file(final_df)
    archive_files()    


#Function to create STG target files from created data frames.
def write_df_to_file(df):
    for source_file_name in list_source_files_names:
        df.write.options(header='True', delimiter=',').mode('overwrite').csv(stg_files_dir_path+'/'+reference_date+'/'+source_file_name)
        for root, dirs, files in os.walk(stg_files_dir_path+'/'+reference_date+'/'+source_file_name):
            # select file name
            for file in files:
                # check the extension of files
                if file.endswith('.csv'):
                    os.rename(stg_files_dir_path+'/'+reference_date+'/'+source_file_name+'/'+file, stg_files_dir_path+'/'+reference_date+'/'+source_file_name+'/'+source_file_name+'.csv')
                    shutil.copy(stg_files_dir_path+'/'+reference_date+'/'+source_file_name+'/'+source_file_name+'.csv', stg_files_dir_path+'/'+reference_date)
                    shutil.rmtree(stg_files_dir_path+'/'+reference_date+'/'+source_file_name, ignore_errors=True)

#Function to move the processed source files to the archive directory.
def archive_files():
    for file_name in os.listdir(source_dir_path):
        shutil.move(source_dir_path+'/'+file_name, arch_dir_path+'/'+reference_date)




