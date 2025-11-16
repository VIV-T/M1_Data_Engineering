### Imports
import logging
import pendulum
import os
import pandas as pd
import re
import numpy as np
import requests
import time
import random as rd

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import timedelta

logger = logging.getLogger(__name__)  # Airflow captures this per task

# --- Failure callback for rich console logs ---
def failure_alert(context):
    exc = context.get("exception")
    ti = context.get("ti")
    logger.error(
        "Task FAILED: dag=%s task=%s run_id=%s try=%s",
        getattr(ti, "dag_id", "?"),
        getattr(ti, "task_id", "?"),
        context.get("run_id"),
        getattr(ti, "try_number", "?"),
    )
    # Full traceback in the task log:
    logger.exception(exc)




### --- DAG config ---
START_DATE = pendulum.datetime(2024, 1, 1, tz="UTC")

# volume related
VOLUME_FOLDER = os.path.join("/opt", "airflow", "project_data")
SCRAPPING_DATA_FOLDER = os.path.join(VOLUME_FOLDER, "scrapping_data")


# request related 
REQUEST_TIMEOUT = 20
MIN_DELAY_S = 0.6
MAX_DELAY_S = 1.2
RETRY_COUNT = 3 # in case of error during requesting : number of retry allowed for one request.
RETRY_BACKOFF = 1.8 # in case of error during requesting : number of second between two retry.
SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0 Safari/537.36"
        )
    }
)

### DAG configuration
with DAG(
    dag_id="ingestion_dag_3",
    start_date=START_DATE,
    schedule=None, 
    catchup=False,
    max_active_tasks=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": failure_alert,
    },
    template_searchpath=["/opt/airflow/data/"]
) as dag:
    
    ### --Tools-- (python_callable)
    #logger.info(os.getcwd())

    # to create a folder in the volume (based on a name and a path in the volume itself)
    def _mkdir_in_volume(folder_name : str, path : str = ""):
        pass

    # To read the csv file as pd.Dataframe 
    def _read_data_file_to_df(source_name : str, additional_name_component = "") :
        data_file_path = os.path.join(SCRAPPING_DATA_FOLDER, "data", f"{source_name}_data{additional_name_component}.csv")
        df_scrapped_data = pd.read_csv(filepath_or_buffer=data_file_path, sep=",")
        return df_scrapped_data
    

    # To save df to csv file persistent in the Docker volume
    def _save_data_file_to_csv(df_to_save : pd.DataFrame, destination_name : str, additional_name_component = "") :
        data_file_path = os.path.join(SCRAPPING_DATA_FOLDER, "data", f"{destination_name}_data{additional_name_component}.csv")
        df_to_save.to_csv(data_file_path, index=False, encoding="utf-8")
        return True



    ## Tools for the HTML Part.
    # To make the program sleep for random time bounded by MIN_DELAY_S and MAX_DELAY_S 
    #       => avoid to be block by the website during the scrapping
    def _sleep_jitter(): 
        time.sleep(rd.uniform(MIN_DELAY_S, MAX_DELAY_S))


    # To fetch the html content of script page 
    # Allow us to handle errors
    def _fetch_html(url: str) -> str: # useful
        last_error = None
        for attempt in range(1, RETRY_COUNT + 1):
            try:
                response = SESSION.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
                response.raise_for_status() # raise an error if the request failed 
                return response.text
            except Exception as e:
                last_error = e
                wait = (RETRY_BACKOFF ** (attempt - 1)) + rd.random() # to have a random wiating time based on the RETRY_BACKOFF variable
                logging.warning(
                    f"[{attempt}/{RETRY_COUNT}] GET failed {url}: {e} | retry in {wait:.1f}s"
                )
                time.sleep(wait)
        raise last_error  # type: ignore


    def extract_html_content(source_name : str):
        # source_name  = "final_scrapping"     # might probably change
        df_data_scrapping = _read_data_file_to_df(source_name=source_name)

        df_html = df_data_scrapping[df_data_scrapping[f"{source_name}_url_extension"]=="html"]
        return True




    ### --Task--
    


    ### --Graph--
    # 1. read the folder 
    # 2. Html side  
    #       a. loop on html urls => use of request to get the data.
    #       b. clean (fastly) the data 
    #       c. save the extracted data
    # 3. Pdf side 
    #       a. loop on pdf urls => use of request to get the data.
    #       b. download the data as file into the volume in the 'ingestion_data' folder (to create with mkdir)
    