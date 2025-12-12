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

with DAG(
    dag_id="ingestion_dag_2",
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

    def to_tropedia_format(title):
        """
        Format a movie title to a Tropedia url format.
        """
        return title.strip().replace(" ", "_").replace(":", "")
    
    def apply_formatting(path_input_file, path_output_file):
        """
        Apply Tropedia formatting to the title column.
        Add 'url' column, with Tropedia url of the movie.
        Add 'exists_on_tropedia' columns and set False by default.
        """

        base_url = "https://tropedia.fandom.com/wiki/"

        logging.info(f"Starting apply_formatting step")

        df = pd.read_csv(path_input_file)

        df["tropedia_name"] = df["movie_name_conventioned"].apply(to_tropedia_format) # Apply naming convention
        df["url_tropedia"] = base_url + df["tropedia_name"] # Create the full url
        df["exists_on_tropedia"] = False # Set the default value to column 'exists_on_tropedia'
        
        df.to_csv(path_output_file, index=False)

        logging.info(f"Finished apply_formatting step")

    def is_on_tropedia(path_input_file):
        """
        Check if the Tropedia page exists for each movie and update the 'exists_on_tropedia' column.
        A page is considered non-existent if it contains the text "There is currently no text in this page".
        """
    
        logging.info(f"Starting is_on_tropedia step")

        df = pd.read_csv(path_input_file)
        df = df.head(5)  # Limit to first 5 rows for testing purposes

        for i, row in df.iterrows():
            try:
                response = requests.get(row["url"], timeout=5) # Ajout d'un timeout
                
                # Check if the page is valid or if the content is empty
                soup = BeautifulSoup(response.text, "html.parser")
                p = soup.select_one("#mw-content-text > div > p")
                
                if p and "There is currently no text in this page" in p.text:
                    df.at[i, "exists_on_tropedia"] = False
                else:
                    df.at[i, "exists_on_tropedia"] = True

            except Exception as e:
                logger.error(f"Error in checking url : {row['url']}")

        df.to_csv(path_input_file, index=False)

        logging.info(f"Finished is_on_tropedia step, the column 'exists_on_tropedia' has been updated.")

    def clean_csv(path_input_file, output_file_scripts, output_file_tropes):
        """Filter the CSV to keep only the movies that exist on Tropedia and drop unnecessary columns.
           The first output file is for scripts data, the second for tropes data.
        """
        logging.info(f"Starting cleaning csv file")

        df_scripts = pd.read_csv(path_input_file)
        df_tropes = pd.read_csv(path_input_file)

        # For scripts data
        df_scripts = df_scripts[df_scripts["exists_on_tropedia"] == True]
        df_scripts = df_scripts.drop(columns=["tropedia_name", "exists_on_tropedia", "url_tropedia"])
        df_scripts.to_csv(output_file_scripts, index=False)

        logging.info("List of scripts movies that exist on Tropedia saved for SCRIPTS part.")

        # For tropes data
        df_tropes = df_tropes[df_scripts["exists_on_tropedia"] == True]
        df_tropes = df_tropes.drop(columns=["tropedia_name", "exists_on_tropedia"]) # WARNING : REMOVE THE COLUMN 
        df_tropes.to_csv(output_file_tropes, index=False)

        logging.info("List of scripts movies that exist on Tropedia saved for TROPES part.")

    def scraping_tropes(path_input_file, path_output_file):
        """
        Scrape tropes from Tropedia for each movie listed in the input CSV file.
        Save the results in a JSON file with the following structure:
        { film_name: 
            [ {trope, url}, ... ],
            ...
                }
        """
        logging.info(f"Starting scraping_tropes step")

        df = pd.read_csv(path_input_file)
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

    def scrape_trope_definitions(path_input_file, path_output_file):
        """
        Scrape the definitions for each unique trope found in the films_tropes_json file.
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
    apply_formatting = PythonOperator(
        task_id="apply_formatting",
        python_callable=apply_formatting,
        op_args=["path/to/input.csv", "path/to/output.csv"], # input = movie list of scripts , # output = movie list of scripts with tropedia attributes
        dag=dag
    )

    is_on_tropedia = PythonOperator(
        task_id="is_on_tropedia",
        python_callable=is_on_tropedia,
        op_args=["path/to/input.csv"], # input = movies list of scripts with tropedia atributes
        dag=dag
    )

    clean_csv = PythonOperator(
        task_id="clean_csv",
        python_callable=clean_csv,
        op_args=["path/to/input.csv", "path/to/output_scripts.csv", "path/to/output_tropes.csv"], # input = movies list of scripts with tropedia atributes , output 1 = scripts data , output 2 = tropedia data
        dag=dag
    )

    scraping_tropes = PythonOperator(
        task_id="scraping_tropes",
        python_callable=scraping_tropes,
        op_args=["path/to/input.csv", "path/to/output.json"], # input = tropedia data , output = movies.json
        dag=dag
    )

    scrape_trope_definitions = PythonOperator(
        task_id="scrape_trope_definitions",
        python_callable=scrape_trope_definitions,
        op_args=["path/to/input.json", "path/to/output.json"], # input = movies.json , output = movies.json
        dag=dag
    )

    ### --Graph--
    apply_formatting >> is_on_tropedia >> clean_csv >> scraping_tropes >> scrape_trope_definitions