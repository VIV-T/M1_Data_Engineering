### Imports
import logging
import pendulum
from docker.types import Mount
import os
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





### --- DAG config ---
START_DATE = pendulum.datetime(2024, 1, 1, tz="UTC")

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
        mounts=[Mount(source='m1_data_engineering_ingestion_data', target='/scrapping/volume/scrapping_data', type='volume')]
    )



    # --Graph--
    launch_scrapping 