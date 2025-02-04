#Importing used packages
import os
import shutil
from dotenv import load_dotenv
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import stg_area


#Loading secured variables from .env file
load_dotenv()
reference_date = os.environ.get('REFERENCE_DATE')
prev_reference_date = os.environ.get('PREV_REFERENCE_DATE')
stg_files_dir_path = os.environ.get('STG_FILES_DIR')

#The start point of DELTA phase
def start_delta():
    store_file_names_with_format()


#Function to store file name and format in a dictionary
def store_file_names_with_format(directory_path):
    file_dict = {}
    for root, dirs, files in os.walk(directory_path):
        for file in files:
            file_path = os.path.join(root, file)
            abs_file_path = os.path.abspath(file_path)
            final_path = abs_file_path.replace("\\","/")
            file_name, file_ext = os.path.splitext(file)
            file_dict[final_path] = file_ext[1:]
    #Creating new dir named with files process date to store stg target files
    if not os.path.exists(stg_files_dir_path+'/'+reference_date):
        os.makedirs(stg_files_dir_path+'/'+reference_date)            
    return file_dict

