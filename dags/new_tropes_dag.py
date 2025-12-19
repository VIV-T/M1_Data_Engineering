### Imports
import logging
import pendulum
from docker.types import Mount
import os
import glob
import pandas as pd
import re
import numpy as np

from bs4 import BeautifulSoup
import requests
import json

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import timedelta

from shared_operators import _volume_mkdir, _read_data_file_to_df, _save_data_file_to_csv


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
VOLUME_FOLDER = os.path.join("/opt", "airflow", "project_data")
SCRAPPING_DATA_FOLDER = os.path.join(VOLUME_FOLDER, "scrapping_data")
INGESTION_DATA_FOLDER = os.path.join(VOLUME_FOLDER, "ingestion_data")

with DAG(
    dag_id="new_tropes_dag",
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


    ### --Task--
    volume_mkdir_ingestion_data_folder = PythonOperator(
        task_id="volume_mkdir_ingestion_data_folder",
        python_callable=_volume_mkdir,
        op_args=[INGESTION_DATA_FOLDER],
        dag=dag
    )


    volume_mkdir_ingestion_data_folder_data = PythonOperator(
        task_id="volume_mkdir_ingestion_data_folder_data",
        python_callable=_volume_mkdir,
        op_args=[os.path.join(INGESTION_DATA_FOLDER, "data")],
        dag=dag
    )


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
            os.path.join(INGESTION_DATA_FOLDER, "data","movie_tropes.json")
        ], 
        dag=dag
    )

    scrap_tropes_definitions = PythonOperator(
        task_id="scrap_tropes_definitions",
        python_callable=_scrap_tropes_definitions,
        # input = movies.json , output = movies.json
        op_args=[
            os.path.join(INGESTION_DATA_FOLDER, "data","movie_tropes.json"),
            os.path.join(INGESTION_DATA_FOLDER, "data","tropes.json")
        ], 
        dag=dag
    )

    ### --Graph--
    volume_mkdir_ingestion_data_folder >> volume_mkdir_ingestion_data_folder_data >> \
    apply_formatting >> is_on_tropedia >> clean_csv >> scrapping_movies_tropes >> scrap_tropes_definitions