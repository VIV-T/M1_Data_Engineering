### Imports
import logging
import pendulum
import os
import pandas as pd
import requests
import time
import random as rd
import json
from bs4 import BeautifulSoup

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.python import PythonOperator
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
    ## TROPES Part.
    def _to_tropedia_format(title):
        """
        Format a movie title to a Tropedia url format.
        """
        logging.info(f"Formatting title: {title}")
        return title.strip().replace(" ", "_").replace(":", "")
    

    def _apply_formatting(input_volume_data_folder, source_name, output_volume_data_folder, destination_name):
        """
        Apply Tropedia formatting to the title column.
        Add 'url' column, with Tropedia url of the movie.
        Add 'exists_on_tropedia' columns and set False by default.
        """

        base_url = "https://tropedia.fandom.com/wiki/"

        logging.info(f"Starting apply_formatting step")

        df = _read_data_file_to_df(volume_data_folder=input_volume_data_folder, source_name=source_name)

        df["tropedia_name"] = df["movie_name_conventioned"].apply(_to_tropedia_format) # Apply naming convention
        df["url_tropedia"] = base_url + df["tropedia_name"] # Create the full url
        df["exists_on_tropedia"] = False # Set the default value to column 'exists_on_tropedia'
        
        # Save this new dataframe
        saved = False 
        while not saved == True :
            saved =_save_data_file_to_csv(volume_data_folder=output_volume_data_folder, destination_name=destination_name, df_to_save=df)

        logging.info(f"Finished apply_formatting step")
        return True


    def _is_on_tropedia(volume_data_folder, source_name, destination_name):
        """
        Check if the Tropedia page exists for each movie and update the 'exists_on_tropedia' column.
        A page is considered non-existent if it contains the text "There is currently no text in this page".
        """
    
        logging.info(f"Starting is_on_tropedia step")

        df = _read_data_file_to_df(volume_data_folder=volume_data_folder, source_name=source_name)
        df = df.head(50)  # Limit to first 5 rows for testing purposes

        for i, row in df.iterrows():
            try:
                response = requests.get(row["url"], timeout=10) # Ajout d'un timeout
                
                # Check if the page is valid or if the content is empty
                soup = BeautifulSoup(response.text, "html.parser")
                p = soup.select_one("#mw-content-text > div > p")
                
                if p and "There is currently no text in this page" in p.text:
                    df.at[i, "exists_on_tropedia"] = False
                else:
                    df.at[i, "exists_on_tropedia"] = True

            except Exception as e:
                logger.error(f"Error in checking url : {row['url']}")

        # Save this new dataframe
        saved = False 
        while not saved == True :
            saved = _save_data_file_to_csv(volume_data_folder=volume_data_folder, destination_name=destination_name, df_to_save=df)

        logging.info(f"Finished is_on_tropedia step, the column 'exists_on_tropedia' has been updated.")
        return True




    def _clean_csv(volume_data_folder, source_name, destination_name_scripts, destination_name_tropes): # path_input_file, output_file_scripts, output_file_tropes
        """Filter the CSV to keep only the movies that exist on Tropedia and drop unnecessary columns.
           The first output file is for scripts data, the second for tropes data.
        """
        logging.info(f"Starting cleaning csv file")
        logging.info(f"Reading source file: {source_name}")
        logging.info(f"Output scripts file: {destination_name_scripts}")
        logging.info(f"Output tropes file: {destination_name_tropes}")

        df = _read_data_file_to_df(volume_data_folder=volume_data_folder, source_name=source_name)

        # For scripts data
        df_scripts = df[df["exists_on_tropedia"] == True]
        df_scripts = df_scripts.drop(columns=["tropedia_name", "exists_on_tropedia", "url_tropedia"])
        # Save this new dataframe
        scripts_saved = False 
        while not scripts_saved == True :
            scripts_saved = _save_data_file_to_csv(volume_data_folder=volume_data_folder, destination_name=destination_name_scripts, df_to_save=df_scripts)

        logging.info("List of scripts movies that exist on Tropedia saved for SCRIPTS part.")
    

        # For tropes data
        df_tropes = df[df["exists_on_tropedia"] == True]
        df_tropes = df_tropes.drop(columns=["tropedia_name", "exists_on_tropedia"]) # WARNING : REMOVE THE COLUMN 
        # Save this new dataframe
        tropes_saved = False 
        while not tropes_saved == True :
            tropes_saved = _save_data_file_to_csv(volume_data_folder=volume_data_folder, destination_name=destination_name_tropes, df_to_save=df_tropes)

        logging.info("List of scripts movies that exist on Tropedia saved for TROPES part.")

        return True



    def _scrapping_movies_tropes(volume_data_folder, source_name, path_output_file):  # path_input_file, path_output_file
        """
        Scrape tropes from Tropedia for each movie listed in the input CSV file.
        Save the results in a JSON file with the following structure:
        { movie_name: 
            [ {trope, url}, ... ],
            ...
                }
        """
        logging.info(f"Starting scraping_tropes step")

        df = _read_data_file_to_df(volume_data_folder=volume_data_folder, source_name=source_name)
        results = {}
        base = "https://tropedia.fandom.com"

        for _, row in df.iterrows():
            film = row["movie_name_conventioned"]
            url = row["url"]

            try:
                response = requests.get(url, timeout=10)
            except Exception as e:
                logger.error(f"HTTP error for {url} : {e}")
                results[film] = []
                continue

            soup = BeautifulSoup(response.text, "html.parser")
            uls = soup.select("#mw-content-text > div ul") #

            film_tropes = []

            # Parse all the <ul> elements found
            for ul in uls:
                for li in ul.find_all("li", recursive=False):
                    a = li.find("a")
                    if a and a.get("href", "").startswith("/wiki/") and "class" not in a.attrs:
                        trope_name = a.text.strip()

                        if trope_name and not trope_name.startswith("Trope_"):
                            film_tropes.append({
                                "trope": trope_name,
                                "url": base + a["href"]
                            })

            results[film] = film_tropes

        with open(path_output_file, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=4, ensure_ascii=False)

        logging.info(f"Finished scraping_tropes step")



    def _scrap_tropes_definitions(path_input_file, path_output_file):
        """
        Scrape the definitions for each unique trope found in the movie_tropes_json file.
        Save the results in a JSON file with the following structure:
        {
            "TropeName": {
                "definition": "..."
            },
            ...
        }
        """

        logging.info(f"Starting scrape_trope_definitions step")
    
        with open(path_input_file, "r", encoding="utf-8") as f:
            films_data = json.load(f)

        # --- Get all unique definitions ---
        unique_tropes = {}  # {trope_name: url}

        for film, tropes in films_data.items():
            for t in tropes:
                name = t["trope"]
                url = t["url"]

                if name not in unique_tropes:
                    unique_tropes[name] = url


        # --- Scrape definitions ---
        trope_definitions = {}

        for name, url in unique_tropes.items():
            
            
            # Errors are handled by storing an empty definition
            try:
                resp = requests.get(url, timeout=10)
            except:
                trope_definitions[name] = {"url": url, "definition": ""}
                continue

            if resp.status_code != 200:
                trope_definitions[name] = {"url": url, "definition": ""}
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            content_div = soup.select_one("#mw-content-text > div")
            definition_parts = []

            # The definition is considered to be all <p> elements until the first <div> or table until the next first <div> or table
            if content_div:
                first_table = content_div.find("table")

                if first_table:
                    for sibling in first_table.next_siblings:
                        if getattr(sibling, "name", None) == "div":
                            break

                        if getattr(sibling, "name", None) == "p":
                            text = sibling.get_text(strip=True)
                            if text and "There is currently no text in this page" not in text:
                                definition_parts.append(text)

            definition = " ".join(definition_parts)

            trope_definitions[name] = {
                "definition": definition
            }

        with open(path_output_file, "w", encoding="utf-8") as f:
            json.dump(trope_definitions, f, indent=4, ensure_ascii=False)

        logging.info(f"Finished scrape_trope_definitions step")



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


        # iter on each row to get te row information (movie_name (to transform dans le ingestion_dag_2 btw) + html_content) (Need the 'source_name' variable to access to the column by their name)
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
    apply_formatting = PythonOperator(
        task_id="apply_formatting",
        python_callable=_apply_formatting,
        # Python args : 
        # input_volume_data_folder, *
        # source_name, 
        # output_volume_data_folder, 
        # destination_name
        op_args=[
            SCRAPPING_DATA_FOLDER, 
            "scrapping", 
            INGESTION_DATA_FOLDER,
            "formated_scrapping"
            ], 
        dag=dag
    )


    is_on_tropedia = PythonOperator(
        task_id="is_on_tropedia",
        python_callable=_is_on_tropedia,
        # Python args : 
        # volume_data_folder (the same for the input and the output here)
        # source_name
        # destination_name
        op_args=[
            INGESTION_DATA_FOLDER, 
            "formated_scrapping", 
            "is_on_tropedia"
        ],
        dag=dag
    )

    clean_csv = PythonOperator(
        task_id="clean_csv",
        python_callable=_clean_csv,
        # Python args 
        # volume_data_folder, 
        # source_name, 
        # destination_name_scripts, 
        # destination_name_tropes
        # Nt : input = movies list of scripts with tropedia atributes , output 1 = scripts data , output 2 = tropedia data
        op_args=[
            INGESTION_DATA_FOLDER, 
            "is_on_tropedia", 
            "ingestion_scripts", 
            "ingestion_tropes"
        ], 
        dag=dag
    )

    scrapping_movies_tropes = PythonOperator(
        task_id="scrapping_movies_tropes",
        python_callable=_scrapping_movies_tropes, # volume_data_folder, source_name, path_output_file
        op_args=[
            INGESTION_DATA_FOLDER, 
            "ingestion_tropes", 
            os.path.join(TROPES_INGESTION_DATA_FOLDER, "movie_tropes.json")
        ], 
        dag=dag
    )

    scrap_tropes_definitions = PythonOperator(
        task_id="scrap_tropes_definitions",
        python_callable=_scrap_tropes_definitions,
        # input = movies.json , output = movies.json
        op_args=[
            os.path.join(TROPES_INGESTION_DATA_FOLDER, "movie_tropes.json"),
            os.path.join(TROPES_INGESTION_DATA_FOLDER, "tropes.json")
        ], 
        dag=dag
    )


    ## HTML and PDF tasks
    extract_and_save_html_content = PythonOperator(
        task_id="extract_and_save_html_content",
        python_callable=_extract_and_save_html_content,
        op_args=[INGESTION_DATA_FOLDER, "ingestion_scripts"],
        dag=dag
    )

    download_pdfs = PythonOperator(
        task_id="download_pdfs",
        python_callable=_download_pdfs,
        op_args=[INGESTION_DATA_FOLDER, "ingestion_scripts"],
        dag=dag
    )


    ### --Graph--
    # 1. read the folder 
    # 2. Html side  
    #       a. loop on html urls => use of request to get the data.
    #       b. save the extracted data
    # 3. Pdf side 
    #       a. loop on pdf urls => use of request to get the data.
    #       b. download the data as file into the volume in the 'ingestion_data' folder (to create with mkdir)
    volume_mkdir_ingestion_data >> volume_mkdir_ingestion_data_data
    
    volume_mkdir_ingestion_data_data >> apply_formatting >> \
    is_on_tropedia >> clean_csv >> [volume_mkdir_ingestion_data_tropes, volume_mkdir_ingestion_data_scripts]
        
    volume_mkdir_ingestion_data_tropes >> scrapping_movies_tropes >> scrap_tropes_definitions 
    
    volume_mkdir_ingestion_data_scripts >> [volume_mkdir_html_data, volume_mkdir_pdf_data]
    volume_mkdir_html_data >> extract_and_save_html_content
    volume_mkdir_pdf_data >> download_pdfs