#Importing used packages
import os
import logging
from datetime import datetime
from dotenv import load_dotenv


#Loading secured variables from .env file
load_dotenv()
log_files_dir_path = os.environ.get('LOG_FILES_DIR')
reference_date = os.environ.get('REFERENCE_DATE')
log_file_path = log_files_dir_path+'/'+reference_date+'_log.txt'

# Define log levels (matching Python's logging module)
LOG_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}

#Creating new dir named with files process date in Logs dir to store log files
def create_log_dir_and_new_log_file():
    if not os.path.exists(log_file_path):
        write_log('Application Started', 'INFO')

# Function to write log messages with log level
def write_log(message, level):
    if level not in LOG_LEVELS:
        raise ValueError(f"Invalid log level: {level}. Valid levels are: {list(LOG_LEVELS.keys())}")
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"{timestamp} - {level} - {message}\n"
    with open(log_file_path, "a") as log_file:
        log_file.write(log_entry)