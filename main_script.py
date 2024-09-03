#Importing used packages
import os
from dotenv import load_dotenv
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
#Our Created script to watch for file occurrence event
from file_listener import *

#Main entry for the script
if __name__ == "__main__":
    
    #File Listener
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