### Imports
import logging
import pendulum
import os
import pandas as pd
import requests
import time
import random as rd
import json
from lxml import html

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from datetime import timedelta

from shared_operators import _read_data_file_to_df, _save_data_file_to_csv, _volume_mkdir

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

# new folder to create (if not existing yet)
INGESTION_DATA_FOLDER = os.path.join(VOLUME_FOLDER, "ingestion_data")
# both - general data folder for ingestion
DATA_INGESTION_DATA_FOLDER = os.path.join(INGESTION_DATA_FOLDER, "data")
# scripts
SCRIPTS_INGESTION_DATA_FOLDER = os.path.join(INGESTION_DATA_FOLDER, "scripts")
HTML_DATA_FOLDER = os.path.join(SCRIPTS_INGESTION_DATA_FOLDER, "html_data")
PDF_DATA_FOLDER = os.path.join(SCRIPTS_INGESTION_DATA_FOLDER, "pdf_data")
# tropes
TROPES_INGESTION_DATA_FOLDER = os.path.join(INGESTION_DATA_FOLDER, "tropes")


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
    dag_id="ingestion_dag",
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

    ## Tropes Part.
    def _scrape_tropes_url(movie_url):
        """Scrap all the tropes url of a movie."""
        try:
            response = requests.get(movie_url, timeout=10)
            response.raise_for_status()
        except Exception as e:
            logging.error(f"Error with {movie_url}: {e}")
            return []

        tree = html.fromstring(response.content)
        # Target url in the tropes list
        elements = tree.xpath('//*[@id="mw-content-text"]/div/ul[1]/li/a')

        tropes = []
        for a in elements:
            trope_name = a.text_content().strip()
            trope_href = a.get("href")
            
            if not trope_href:
                continue

            # Be sure the URL is complete
            base_url = "https://tropedia.fandom.com"
            full_url = trope_href if trope_href.startswith("http") else base_url + trope_href

            tropes.append({
                "trope_name": trope_name,
                "url": full_url
            })
        return tropes
    

    def _scrape_trope_definition(trope_url):
        """Scrape the definition of a trope."""
        try:
            response = requests.get(trope_url, timeout=10)
            response.raise_for_status()
            tree = html.fromstring(response.content)
            
            # Extraction of the first significant paragraph outside tables (infobox)
            paragraphs = tree.xpath('//*[@id="mw-content-text"]/div/p[not(ancestor::table)]')
            if paragraphs:
                for p in paragraphs:
                    text = p.text_content().strip()
                    # Ignore empty or too short paragraphs
                    if text and len(text) > 30:
                        return text
            logging.info(f"DDefinition not found for {trope_url}")
        except Exception as e:
            logging.error(f"Error of scraping: {str(e)}")

    
    def _build_unique_tropes_definitions(volume_data_folder, source_name, final_output_json):
        """
        1. Scrape trope data for all movies from the CSV.
        2. Extract a unique list of tropes via a dictionary (unique key).
        3. Scrape the definition of each unique trope.
        4. Save everything in a JSON.
        """
        
        df = _read_data_file_to_df(volume_data_folder=volume_data_folder, source_name=source_name)
        # This dictionary guarantees that there are no duplicates : { "Nom": "URL" }
        all_unique_tropes = {} 

        # STEP 1 : Collecting all unique tropes
        logging.info(f"--- Phase 1 : Collecting unique tropes on {len(df)} movies ---")
        for index, row in df.iterrows():
            logging.info(f"[{index+1}/{len(df)}] Extraction for : {row['movie_name_conventioned']}")
            movie_tropes = _scrape_tropes_url(row["url_tropedia"])
            
            for t in movie_tropes:
                # If the trope already exists, the dictionary does not create a new entry.
                if t["trope_name"] not in all_unique_tropes:
                    all_unique_tropes[t["trope_name"]] = t["url"]

        logging.info(f"\nTotal number of unique tropes to scrape : {len(all_unique_tropes)}")

        # STEP 2 : Scraping of definitions (one call per unique trope)
        logging.info(f"--- Phase 2 : Scraping of definitions ---")
        final_result = {}
        count = 0
        total = len(all_unique_tropes)

        for name, url in all_unique_tropes.items():
            count += 1
            logging.info(f"[{count}/{total}] Scrapping definition : {name}")
            definition = _scrape_trope_definition(url)
            
            final_result[name] = {
                "definition": definition
            }
           
            time.sleep(0.1)

        # STEP 3 : Final saving
        with open(final_output_json, "w", encoding="utf-8") as f:
            json.dump(final_result, f, indent=4, ensure_ascii=False)

        logging.info(f"\nDone ! {len(final_result)} tropes saved in {final_output_json}")
        return final_result


    ## HTML and PDF Part.
    ## Tools for the HTML Part.
    # To make the program sleep for random time bounded by MIN_DELAY_S and MAX_DELAY_S 
    #       => avoid to be block by the website during the scrapping
    def _sleep_jitter(): 
        time.sleep(rd.uniform(MIN_DELAY_S, MAX_DELAY_S))


    # To fetch the html content of script page 
    # Allow us to handle errors
    def _fetch_html_to_txt(url: str) -> str: 
        _sleep_jitter() # add a random waiting time to avoid to be blocked by the site.

        for attempt in range(1, RETRY_COUNT + 1):
            try:
                response = SESSION.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
                response.raise_for_status() # raise an error if the request failed 
                logging.info(f"_fetch_html_to_txt - New url fetched : {url}")
                return response.text
            except Exception as e:
                wait = (RETRY_BACKOFF ** (attempt - 1)) + rd.random() # to have a random waiting time based on the RETRY_BACKOFF variable
                logging.warning(
                    f"_fetch_html_to_txt - [{attempt}/{RETRY_COUNT}] GET failed {url}: {e} | retry in {wait:.1f}s"
                )
                time.sleep(wait)

        # error case management
        logging.error(f"_fetch_html_to_txt - Couldn't fetch this url : {url}")
        return None


    # Based on the scrapped data : we fetch the html content and save it into a file in the Docker volume.
    def _extract_and_save_html_content(volume_data_folder : str, source_name : str):
        # source_name  = "final_scrapping"
        df_data_scrapping = _read_data_file_to_df(volume_data_folder=volume_data_folder, source_name=source_name)

        # filter the df to keep only html urls
        df_html = df_data_scrapping[df_data_scrapping["url_extension"]=="html"]

        # get the html content into the df
        df_html["content"] = df_html["url"].apply(_fetch_html_to_txt)

        # iter on each row to get te row information
        for index, row in df_html.iterrows():
            filename = row["filename"]
            file_path = os.path.join(HTML_DATA_FOLDER, filename)
            try : 
                # write the html content in a dedicated file in the volume.
                with open(file = file_path, mode="w", encoding="utf-8") as f:
                    f.write(row["content"])
                logging.info(f"_extract_and_save_html_content - {filename} saved")
            except Exception as e:
                logging.error(f"_extract_and_save_html_content - Error while saving : {filename} - {e}")

        return True
    

    # To download the pdf in the Docker volume based on the pdf urls.
    def _download_pdfs(volume_data_folder : str, source_name :str):
        # source_name  = "final_scrapping"
        df_data_scrapping = _read_data_file_to_df(volume_data_folder=volume_data_folder, source_name=source_name)

        # filter the df to keep only html urls
        df_pdf = df_data_scrapping[df_data_scrapping["url_extension"]=="pdf"]

        for index, row in df_pdf.iterrows() :
            url = row["url"]
            filename = row["filename"]
            file_path = os.path.join(PDF_DATA_FOLDER, filename)
            try:
                response = requests.get(url, stream=True, timeout=REQUEST_TIMEOUT, headers={"User-Agent": "Mozilla/5.0"})
                # error based the HTTP status
                if response.status_code != 200:
                    logging.error(f"_download_pdf - HTTP {response.status_code} for {url}")
                    
                # case where everything works well
                with open(file_path, "wb") as f:
                    for chunk in response.iter_content(128 * 1024):
                        if chunk:
                            f.write(chunk)
                logging.info(f"_download_pdf - Pdf downloaded from : {url}")
                
            # any other error case
            except Exception as e:
                logging.error(f"_download_pdf - Error for {url}: {e}")
                

    ### --Task--
    ## Volume mkdir tasks
    volume_mkdir_ingestion_data = PythonOperator(
        task_id="volume_mkdir_ingestion_data",
        python_callable=_volume_mkdir,
        op_args=[INGESTION_DATA_FOLDER],
        dag=dag
    )

    volume_mkdir_ingestion_data_data = PythonOperator(
        task_id="volume_mkdir_ingestion_data_data",
        python_callable=_volume_mkdir,
        op_args=[DATA_INGESTION_DATA_FOLDER],
        dag=dag
    )

    volume_mkdir_ingestion_data_scripts = PythonOperator(
        task_id="volume_mkdir_ingestion_data_scripts",
        python_callable=_volume_mkdir,
        op_args=[SCRIPTS_INGESTION_DATA_FOLDER],
        dag=dag
    )

    volume_mkdir_ingestion_data_tropes = PythonOperator(
        task_id="volume_mkdir_ingestion_data_tropes",
        python_callable=_volume_mkdir,
        op_args=[TROPES_INGESTION_DATA_FOLDER],
        dag=dag
    )
    
    volume_mkdir_html_data = PythonOperator(
        task_id="volume_mkdir_html_data",
        python_callable=_volume_mkdir,
        op_args=[HTML_DATA_FOLDER],
        dag=dag
    )

    volume_mkdir_pdf_data = PythonOperator(
        task_id="volume_mkdir_pdf_data",
        python_callable=_volume_mkdir,
        op_args=[PDF_DATA_FOLDER],
        dag=dag
    )


    ## Tropes tasks
    ingest_tropes_definitions = PythonOperator(task_id="ingest_tropes_definitions",
        python_callable=_build_unique_tropes_definitions, # volume_data_folder, source_name, path_output_file
        op_args=[
            SCRAPPING_DATA_FOLDER, 
            "ingestion_tropes", 
            os.path.join(TROPES_INGESTION_DATA_FOLDER, "movie_tropes.json")
        ], 
        dag=dag
    )

 
    ## Script tasks - HTML and PDF 
    extract_and_save_html_content = PythonOperator(
        task_id="extract_and_save_html_content",
        python_callable=_extract_and_save_html_content,
        op_args=[SCRAPPING_DATA_FOLDER, "ingestion_scripts"],
        dag=dag
    )

    download_pdfs = PythonOperator(
        task_id="download_pdfs",
        python_callable=_download_pdfs,
        op_args=[SCRAPPING_DATA_FOLDER, "ingestion_scripts"],
        dag=dag
    )

    end = EmptyOperator(task_id="end")

    ### --Graph--
    # 1. read the folder 
    # 2. Html side  
    #       a. loop on html urls => use of request to get the data.
    #       b. save the extracted data
    # 3. Pdf side 
    #       a. loop on pdf urls => use of request to get the data.
    #       b. download the data as file into the volume in the 'ingestion_data' folder (to create with mkdir)
    volume_mkdir_ingestion_data >> volume_mkdir_ingestion_data_data
    
    volume_mkdir_ingestion_data_data >> [volume_mkdir_ingestion_data_tropes, volume_mkdir_ingestion_data_scripts]
        
    #volume_mkdir_ingestion_data_tropes >> ingest_movies_tropes >> ingest_tropes_definitions 
    volume_mkdir_ingestion_data_tropes >> ingest_tropes_definitions >> end

    volume_mkdir_ingestion_data_scripts >> [volume_mkdir_html_data, volume_mkdir_pdf_data]
    volume_mkdir_html_data >> extract_and_save_html_content >> end
    volume_mkdir_pdf_data >> download_pdfs >> end
