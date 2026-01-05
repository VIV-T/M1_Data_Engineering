### Imports
import logging
import pendulum
from docker.types import Mount
import os
import glob

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import timedelta

from shared_operators import _create_collection, _volume_mkdir, _connect_mongoDB, _save_data_file_to_csv, _read_data_file_to_df

logger = logging.getLogger(__name__)


# volume related
VOLUME_FOLDER = os.path.join("/opt", "airflow", "project_data")
# new folder to create (if not existing yet)
STAGGING_DATA_FOLDER = os.path.join(VOLUME_FOLDER, "stagging_data")
STAGGING_DATA_LOGS_FOLDER = os.path.join(STAGGING_DATA_FOLDER, "logs")
STAGGING_DATA_SCRIPTS_FOLDER = os.path.join(STAGGING_DATA_FOLDER, "scripts")

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

with DAG(
    dag_id="production_dag",
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
    
    # --Tools-- (python_callable)


    # --Tasks--


    # --Graph--

    pass

    ## Steps to implement in the DAG: Global structure
    # 1. Read data :
        # - Tropes database from JSON file (MongoDB collection)
        # - Scripts & scenes from the MongoDB collection

    # 2. Encode trope definitions with Sentence-BERT:
        # - Load SentenceTransformer model
        # - Encode trope definitions to get embeddings

    # 3. Create FAISS index for vector search:
        # - Initialize FAISS index
        # - Add trope embeddings to the index

    # 4. Retrieve relevant tropes:
        # - Define a PythonOperator to retrieve relevant tropes for each script segment using FAISS

    # 5. Load generative model (Mistral-7B):
        # - Load tokenizer and model using transformers library

    # 6. Generate responses with the model:
        # - Define a PythonOperator to generate responses based on retrieved tropes and script segments

    # 7. Format & save results in the expected output for analysis:
        # - Define a PythonOperator to save the analysis results back to MongoDB or as CSV

    ## Notes: 
    # - Divide the local & the global analysis into separate tasks/operators
    #      => Local analysis on script segments, Global analysis on the full script
    #      - For local analysis, iterate over scenes/segments stored in MongoDB using Pyspark ?
    # - Use of a dedicated container for heavy tasks (model loading, inference) ? DockerOperator
    # - Ensure proper logging and error handling throughout the DAG


    ## DAG structure:
    # volume_mkdir (necessary folders)
    # DockerOperator to launch the script analysis container
    #     - Inside the container, implement the steps mentioned above (reading data, encoding, indexing, retrieval, generation)
    #     - Save results to MongoDB or PostgreSQL
    # Perform some analysis on the results if needed (querying, aggregations with SQL or Pyspark) depend on the chosen storage solution & data format.


    ## General notes & remarks about the project:
    # - Add another data source for genre and ratings of movies (IMDB API ?) to improve analysis dimensions.
    #       => or any kind of remevant information about the movies to enrich the analysis.
    # - Use of a PostgreSQL database for production instead of MongoDB for better reliability and performance? (and easy querying)
    #       => If yes, define the star schema for the database.