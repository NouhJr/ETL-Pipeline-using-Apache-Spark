#Importing used packages
import os
from dotenv import load_dotenv
from create_logs import write_log
from google.cloud import bigquery
from google.oauth2 import service_account
from Spark_Logic.spark_initialize import load_dataframe_from_gcp

load_dotenv()
service_account_file = os.environ.get('SERVICE_ACCOUNT_FILE')
project_id = os.environ.get('PROJECT_ID')
reference_date = os.environ.get("REFERENCE_DATE")

def upload_dataframes_to_bigquery(data_set_id, data_frame, table_name, upload_type):
    write_log("Connecting to GCP....", "INFO")
    # Convert spark dataframe to pandas data frame to load it in big query.
    pandas_df = data_frame.toPandas()
    # Authenticate with service account
    credentials = service_account.Credentials.from_service_account_file(
        service_account_file,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    # Initialize BigQuery client
    client = bigquery.Client(credentials=credentials, project=project_id)

    if upload_type == "stg":
        # Delete existing data with the specific reference date
        write_log(f"Running pre SQL statement on table: {table_name.lower()} ....", "INFO")
        table_id = f"{project_id}.{data_set_id}.{table_name.lower()}"
        try:
            client.get_table(table_id)  # Check if the table exists
            query = f"""
                DELETE FROM `{table_id}`
                WHERE REFERENCE_DATE = DATE('{reference_date}')
            """
            pre_sql_job_config = bigquery.QueryJobConfig()
            delete_job = client.query(query, job_config=pre_sql_job_config)
            delete_job.result()
            write_log(f"Pre SQL statement finished successfully.", "INFO")
        except Exception as e:
            if "Not found" in str(e):
                write_log(f"Table: {table_name.lower()}, does not exist. Skipping delete operation.", "INFO")
            else:
                write_log(f"{str(e)}", "ERROR")
                write_log(f"Application terminated.", "INFO")
                exit()
                raise

    # Configure job for loading data
    upload_job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    write_log(f"Writing data from: {table_name}, to BigQuery table....", "INFO")
    try:
        table_ref = client.dataset(data_set_id).table(table_name.lower())
        job = client.load_table_from_dataframe(pandas_df, table_ref, upload_job_config)
        job.result()
        write_log(f"Uploaded {len(pandas_df)} rows to table: {table_ref}", "INFO")
    except Exception as e:
        write_log(f"{str(e)}", "ERROR")
        write_log(f"Application terminated.", "INFO")
        exit()
        raise    


def load_bigquery_tables_to_df(data_set_id, list_files_names):
    write_log("Loading data from BigQuery....", "INFO")
    dataframes = []
    try:
        for source_file_name in list_files_names:
            table_name = source_file_name.lower()
            table = f"{data_set_id}.{table_name}"
            df = load_dataframe_from_gcp(table)
            dataframes.append(df)
        write_log("Data loaded successfully.", "INFO")
        return dataframes    
    except Exception as e:
        write_log(f"{str(e)}", "ERROR")
        write_log(f"Application terminated.", "INFO")
        exit()
        raise    