#Importing used packages
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pathlib import Path
from dotenv import load_dotenv
#Our Created script to watch for file occurrence event
from file_listener import *
from stg_area import *

#Main entry for the script
if __name__ == "__main__":
    #Loading secured variables from .env file
    load_dotenv()
    source_files_dir_path = os.environ.get('SOURCE_FILES_DIR')

    #Check if the directory contains files or not
    is_empty = not any(Path(source_files_dir_path).iterdir())
    
    #Listen for new files
    if is_empty:
        #File Listener
        listen_for_file()
    #Work on existing files and listen for new files
    else:
        get_files_info(MyHandler.store_file_names_with_format(source_files_dir_path))
        listen_for_file()   