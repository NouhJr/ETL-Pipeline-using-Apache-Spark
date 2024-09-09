#Importing used packages
import os
from dotenv import load_dotenv
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from stg_area import *

#Loading secured variables from .env file
load_dotenv()
source_files_dir_path = os.environ.get('SOURCE_FILES_DIR')
source_files_arch_dir_path = os.environ.get('SOURCE_FILES_ARCHIVE_DIR')
reference_date = os.environ.get('REFERENCE_DATE')

#Function to wait for new files.
def listen_for_file():
    observer = Observer()
    event_handler = MyHandler()
    observer.schedule(event_handler, path= source_files_dir_path, recursive=False)
    observer.start()
    try:
        while True:
            pass
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

#File Listener class
class MyHandler(FileSystemEventHandler):
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
        #Creating new dir named with files process date to archive source files
        os.makedirs(source_files_arch_dir_path+'/'+reference_date)        
        return file_dict

    #Function to wait for file occurrence event and apply actions to it.
    def on_created(self, event):
        if not event.is_directory:
            get_files_info(MyHandler.store_file_names_with_format(source_files_dir_path))