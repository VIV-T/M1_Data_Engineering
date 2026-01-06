### Imports
import logging
import pendulum
from docker.types import Mount
import os
import glob

from sentence_transformers import SentenceTransformer
import faiss

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

PRODUCTION_DATA_FOLDER = os.path.join(VOLUME_FOLDER, "production_data")
TOOLS_FOLDER = os.path.join(PRODUCTION_DATA_FOLDER, "tools")
PRODUCTION_LOGS_FOLDER = os.path.join(PRODUCTION_DATA_FOLDER, "logs")

MODEL_PATH = os.path.join(TOOLS_FOLDER, "bert_model")
FAISS_INDEX_PATH =  os.path.join(TOOLS_FOLDER, "faiss_index.faiss")

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
    def _read_tropes_data():
        # connect to Mongo DB to read the tropes documents inside the "tropes" collection
        scripts_db = _connect_mongoDB()
        tropes_collection = scripts_db["tropes"]

        tropes_list = {doc["name"] : doc["definition"] for doc in tropes_collection.find({}, {"name" : 1, "definition" : 1, "_id" : 0})}

        return tropes_list


    def _create_faiss_index():
        # Read the trope list from MongoDB
        tropes_list = _read_tropes_data()

        # Encode tropes definition with Sentence-BERT
        model_embedding = SentenceTransformer('bert-base-nli-stsb-mean-tokens')
        model_embedding.save(MODEL_PATH)    # save the model to be able to re-use it when the embedding of script and scene is needed
        trope_definitions = [definition for definition in tropes_list.values()]
        trope_embeddings = model_embedding.encode(trope_definitions)

        # Create Faiss indexes for vectorial research
        dimension = trope_embeddings.shape[1]
        index = faiss.IndexFlatL2(dimension)
        index.add(trope_embeddings)

        # save faiss index
        faiss.write_index(index, FAISS_INDEX_PATH)   # to load : var = faiss.read_index(FAISS_INDEX_PATH)

    

    # --Tasks--
    volume_mkdir_production_data = PythonOperator(
        task_id="volume_mkdir_production_data",
        python_callable=_volume_mkdir,
        op_args=[PRODUCTION_DATA_FOLDER],
        dag=dag
    )

    volume_mkdir_tools = PythonOperator(
        task_id="volume_mkdir_tools",
        python_callable=_volume_mkdir,
        op_args=[TOOLS_FOLDER],
        dag=dag
    )

    volume_mkdir_logs = PythonOperator(
        task_id="volume_mkdir_logs",
        python_callable=_volume_mkdir,
        op_args=[PRODUCTION_LOGS_FOLDER],
        dag=dag
    )

    create_faiss_index = PythonOperator(
        task_id="create_faiss_index",
        python_callable=_create_faiss_index,
        op_args=[],
        dag=dag
    )

    launch_analysis_container = DockerOperator(task_id='launch_stagging_container',
        container_name="analysis_container",
        image='m1_data_engineering-analyser:latest',   # use the docker image build by the 'scrapper' service in the docker-compose.yml
        command=["/opt/venv_analysis/bin/python", "/app/analysis_container/analysis_main.py"],
        api_version='auto',
        auto_remove="success",    # set to 'never' to check the logs or 'success' in normal case
        docker_url='tcp://docker-proxy:2375', # use the proxy service set in the docker-compose.yml
        network_mode="airflow_network",
        mount_tmp_dir=False,
        dag=dag,
        user='root',
        environment={
            "MODEL_PATH" : MODEL_PATH,
            "FAISS_INDEX_PATH" : FAISS_INDEX_PATH
        },

        # Synchronize a volume between the scrapper container and the airflow container
        mounts=[Mount(source='m1_data_engineering_project_data', target='/app/project_data', type='volume')]
    )

    # --Graph--
    volume_mkdir_production_data >> [volume_mkdir_tools, volume_mkdir_logs]
    volume_mkdir_tools >> create_faiss_index
    [volume_mkdir_logs, create_faiss_index] >> launch_analysis_container


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