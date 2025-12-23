### Imports
import logging
import pendulum
from docker.types import Mount
import os

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import timedelta

logger = logging.getLogger(__name__)  # Airflow captures this per task


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
    dag_id="preparation_dag",
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
    # To create a folder in the volume : the path must ti be already created
    def _volume_mkdir(folder_path : str) : 
        # it doesn't create a folder twice if it already exist in the volume.
        os.makedirs(folder_path, exist_ok=True)

    # --Task--
    launch_preparation_container = DockerOperator(
        task_id='launch_preparation_container',
        container_name="preparation_container",
        image='m1_data_engineering-data_preparer:latest',   # use the docker image build by the 'scrapper' service in the docker-compose.yml
        command=["/opt/venv_test/bin/python", "/app/preparation_container/app/main.py"],
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



    # --Graph--
    launch_preparation_container