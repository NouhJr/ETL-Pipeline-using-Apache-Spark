#Importing used packages
import os
from dotenv import load_dotenv
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

#Loading secured variables from .env file
load_dotenv()
source_files_dir_path = os.environ.get('SOURCE_FILES_DIR')

#File Listener class
class MyHandler(FileSystemEventHandler):
    #Function to store file name and format in a dictionary
    def store_file_names_with_format(directory_path):
        file_dict = {}
        for root, dirs, files in os.walk(directory_path):
            for file in files:
                file_path = os.path.join(root, file)
                file_name, file_ext = os.path.splitext(file)
                file_dict['file_name'] = file_name 
                file_dict['file_format'] = file_ext[1:]
        return file_dict

    #Function to wait for file occurrence event and apply actions to it.
    def on_created(self, event):
        if not event.is_directory:
            MyHandler.store_file_names_with_format(source_files_dir_path)