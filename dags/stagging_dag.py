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

from dags.segmentation import main_segmentation
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
    dag_id="stagging_dag",
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
    # to define here any python function used in the tasks if not imported from shared_operators.py
    def _save_scripts_to_mongodb(): 
        script_files = glob.glob(f"{STAGGING_DATA_SCRIPTS_FOLDER}/*.txt")
        scripts_db = _connect_mongoDB()

        scripts_collection = scripts_db["movies"]
        for file_path in script_files:
            with open(file_path, "r", encoding="utf-8") as f:
                script_content = f.read()
                movie_name = (os.path.basename(file_path)).replace(".txt", "")
                script_document = {
                    "file_name": movie_name,
                    "full_script": script_content
                }
                scripts_collection.insert_one(script_document)
                logger.info(f"Inserted script of {movie_name} into MongoDB")

    # --Task--
    volume_mkdir_stagging_data = PythonOperator(
        task_id="volume_mkdir_stagging_data",
        python_callable=_volume_mkdir,
        op_args=[STAGGING_DATA_FOLDER],
        dag=dag
    )

    volume_mkdir_stagging_data_logs = PythonOperator(
        task_id="volume_mkdir_stagging_data_logs",
        python_callable=_volume_mkdir,
        op_args=[STAGGING_DATA_LOGS_FOLDER],
        dag=dag
    )

    volume_mkdir_stagging_data_scripts = PythonOperator(
        task_id="volume_mkdir_stagging_data_scripts",
        python_callable=_volume_mkdir,
        op_args=[STAGGING_DATA_SCRIPTS_FOLDER],
        dag=dag
    )


    # To extract content from the PDF and HTML files and save it into the docker volume as txt files.
    launch_stagging_container = DockerOperator(
        task_id='launch_stagging_container',
        container_name="stagging_container",
        image='m1_data_engineering-stagger:latest',   # use the docker image build by the 'scrapper' service in the docker-compose.yml
        command=["/opt/venv_stagging/bin/python", "/app/stagging_container/stagging_files/stagging_main.py"],
        api_version='auto',
        auto_remove="success",    # set to 'never' to check the logs or 'success' in normal case
        docker_url='tcp://docker-proxy:2375', # use the proxy service set in the docker-compose.yml
        network_mode="airflow_network",
        mount_tmp_dir=False,
        dag=dag,
        user='root',

        # Synchronize a volume between the scrapper container and the airflow container
        mounts=[Mount(source='m1_data_engineering_project_data', target='/app/project_data', type='volume')]
    )


    ## MongoDB related tasks
    # Check the MongoDB connection
    check_mongoDB_connection = PythonOperator(
        task_id="check_mongoDB_connection",
        python_callable=_connect_mongoDB,
        dag=dag
    )

    # Create the collection to store the scripts in MongoDB
    create_scripts_collection = PythonOperator(
        task_id="create_scripts_collection",
        python_callable=_create_collection,  
        op_args=["movies"],
        dag=dag
    )

    # Save the scripts content into the MongoDB database.
    save_scripts_to_mongodb = PythonOperator(
        task_id="save_scripts_to_mongodb",
        python_callable=_save_scripts_to_mongodb,  
        dag=dag
    )
    

    # Then add the segmentation task here + Maj on MongoDB collection with segemented scenes
    segment_scripts = PythonOperator(
        task_id="segment_scripts",
        python_callable=main_segmentation,  
        dag=dag
    )

    # --Graph--
    volume_mkdir_stagging_data >> [volume_mkdir_stagging_data_logs, volume_mkdir_stagging_data_scripts] \
    >> launch_stagging_container >> check_mongoDB_connection >> create_scripts_collection \
    >> save_scripts_to_mongodb >> segment_scripts
