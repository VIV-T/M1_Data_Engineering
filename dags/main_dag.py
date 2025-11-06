### Imports
import logging
import pendulum
from docker.types import Mount
import os
from pymongo import MongoClient
import glob
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

# test if Airflow is able to read the file
def _get_json_files(collection_name : str) : 
    try : 
        script_files = glob.glob(f'/opt/airflow/data_scrapping/scripts/{collection_name}/*.json')
        logger.info("Success reading the files")
        logger.info(f"Files found: {script_files}")
        return script_files
    except Exception as e:
        logger.error(f"Error reading the files: {e}")


# Connection to MongoDB
def _connect_mongoDB() : 
        client = MongoClient(
            host= f"mongodb://mongo:27017/",
            username="admin",
            password="admin"
        )
        logger.info("MongoDB client created")
        logger.info(f"Databases available: {client.list_database_names()}") 
        scripts_db = client["scriptsDB"]
        return scripts_db

# check connection to MongoDB
def _check_connection_mongoDB() :
    try :
        _connect_mongoDB()
        logger.info("Success connecting to MongoDB")
    except Exception as e:
        logger.error(f"Error connecting to MongoDB: {e}")
        exit(1)


# create a collection in MongoDB
def _create_collection(collection_name : str) :
    scripts_db = _connect_mongoDB()
    # check if the collection already exists
    logger.info(f"{scripts_db.list_collection_names()}")

    # logger.info(f"{scripts_db.list_collection_names()}")
    try : 
        if not collection_name in scripts_db.list_collection_names() :   
            scripts_collection = scripts_db[f"{collection_name}"]
            logger.info("Success creating the collection in MongoDB")
        else : 
            logger.info("The collection already exists in MongoDB")
    except Exception as e:
        logger.error(f"Error creating the collection in MongoDB: {e}")


# insert data in a collection in MongoDB
def _insert_in_collection(collection_name : str) : 
    scripts_db = _connect_mongoDB()
    scripts_filelist = _get_json_files(collection_name)

    try : 
        scripts_collection = scripts_db[f"{collection_name}"]
        logger.info(f"script_files found: {scripts_filelist}")
        for script_file in scripts_filelist :
            with open(script_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            scripts_collection.insert_one(data)
        
        logger.info("Success inserting data in the collection in MongoDB")
    except Exception as e:
        logger.error(f"Error inserting data in the collection in MongoDB: {e}")


# --- DAG config ---
START_DATE = pendulum.datetime(2024, 1, 1, tz="UTC")

with DAG(
    dag_id="main_dag",
    start_date=START_DATE,
    schedule="0 0 * * *",         # daily at 00:00 UTC
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
    logger.info(os.getcwd())

    # --Task--
    launch_scrapping = DockerOperator(
        task_id='launch_scrapping_container',
        image='m1_data_engineering-scrapper:latest',   # use the docker image build by the 'scrapper' service in the docker-compose.yml
        api_version='auto',
        auto_remove="success",    # set to 'never' to check the logs or 'success' in normal case
        docker_url='tcp://docker-proxy:2375', # use the proxy service set in the docker-compose.yml
        network_mode="airflow_network",
        mount_tmp_dir=False,
        dag=dag,

        # Synchronize a volume between the scrapper container and the airflow container
        mounts=[Mount(source='m1_data_engineering_scrapper_data', target='/scrapping/data_scripts', type='volume')]
    )

    check_mongo_connection = PythonOperator(
        task_id='check_mongo_connection',
        python_callable=_check_connection_mongoDB,
        dag=dag
    )

    create_imsdb_collection = PythonOperator(
        task_id='create_imsdb_collection',
        python_callable=_create_collection,
        op_args=['imsdb'],
        dag=dag
    )

    insert_imsdb_collection = PythonOperator(
        task_id='insert_imsdb_collection',
        python_callable=_insert_in_collection,
        op_args=['imsdb'],
        dag=dag
    )

    create_simplyScripts_collection = PythonOperator(
        task_id='create_simplyScripts_collection',
        python_callable=_create_collection,
        op_args=['simplyScripts'],
        dag=dag
    )

    insert_simplyScripts_collection = PythonOperator(
        task_id='insert_simplyScripts_collection',
        python_callable=_insert_in_collection,
        op_args=['simplyScripts'],
        dag=dag
    )


    # --Graph--
    launch_scrapping >> \
    check_mongo_connection >> [create_imsdb_collection, create_simplyScripts_collection]
    create_imsdb_collection >> insert_imsdb_collection
    create_simplyScripts_collection >> insert_simplyScripts_collection 