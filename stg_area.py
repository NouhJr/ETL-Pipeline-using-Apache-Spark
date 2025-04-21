#Importing used packages
import os
from spark_logic import load_dataframe, stagging_logic


#Function to get file path and file format to use it to create new spark dataframe from this file.
list_dfs = []
list_source_files_names = []
def get_files_info(files_dict):
    for file_name in files_dict:
        list_dfs.append(load_dataframe(file_name, files_dict[file_name]))
        file_name_txt = os.path.basename(file_name)
        file = os.path.splitext(file_name_txt)
        list_source_files_names.append(file[0])
    # Calling the stagging logic function to process the dataframes and upload them to big query.   
    stagging_logic(list_dfs, list_source_files_names)    

    




