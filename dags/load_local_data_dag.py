### Imports
import logging
import pendulum
from docker.types import Mount
import os
import glob

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from datetime import timedelta

from shared_operators import _create_collection, _volume_mkdir, _connect_mongoDB, _save_data_file_to_csv, _read_data_file_to_df

logger = logging.getLogger(__name__)


# volume related
VOLUME_FOLDER = os.path.join("/opt", "airflow", "project_data")
LOCAL_DATA_FOLDER = os.path.join("/opt", "airflow", "data")

# new folder to create (if not existing yet)
# ingestion_data folder and subfolders
INGESTION_DATA_FOLDER = os.path.join(VOLUME_FOLDER, "ingestion_data")
SCRIPTS_FOLDER = os.path.join(INGESTION_DATA_FOLDER, "scripts")
TROPES_FOLDER = os.path.join(INGESTION_DATA_FOLDER, "tropes")
PDF_FOLDER = os.path.join(INGESTION_DATA_FOLDER, "scripts", "pdf_data")
HTML_FOLDER = os.path.join(INGESTION_DATA_FOLDER, "scripts", "html_data")


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
    dag_id="load_local_data_dag",
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
    def _duplicate_data_to_volume() : 
        # scripts
        local_pdf_folder = os.path.join(LOCAL_DATA_FOLDER, "scripts", "pdf_data")
        local_html_folder = os.path.join(LOCAL_DATA_FOLDER, "scripts", "html_data")

        # copy pdf files
        for pdf_file in glob.iglob(f"{local_pdf_folder}/*"):
            dest_path = os.path.join(PDF_FOLDER, os.path.basename(pdf_file))
            os.system(f"cp {pdf_file} {dest_path}")

        # copy html files
        for html_file in glob.iglob(f"{local_html_folder}/*"):
            dest_path =  os.path.join(HTML_FOLDER, os.path.basename(html_file))
            os.system(f"cp {html_file} {dest_path}")


        # tropes list
        local_tropes_file = os.path.join(LOCAL_DATA_FOLDER, "tropes", "movie_tropes.json")
        dest_tropes_file = os.path.join(TROPES_FOLDER, "movie_tropes.json")
        os.system(f"cp {local_tropes_file} {dest_tropes_file}")
        logger.info("All local data duplicated to the volume folders.")


    # --Tasks--
    volume_mkdir_ingestion_data = PythonOperator(
        task_id="volume_mkdir_ingestion_data",
        python_callable=_volume_mkdir,
        op_args=[INGESTION_DATA_FOLDER],
        dag=dag
    )

    volume_mkdir_ingestion_data_scripts = PythonOperator(
        task_id="volume_mkdir_ingestion_data_scripts",
        python_callable=_volume_mkdir,
        op_args=[SCRIPTS_FOLDER],
        dag=dag
    )

    volume_mkdir_ingestion_data_pdf = PythonOperator(
        task_id="volume_mkdir_ingestion_data_pdf",
        python_callable=_volume_mkdir,
        op_args=[PDF_FOLDER],
        dag=dag
    )

    volume_mkdir_ingestion_data_html = PythonOperator(
        task_id="volume_mkdir_ingestion_data_html",
        python_callable=_volume_mkdir,
        op_args=[HTML_FOLDER],
        dag=dag
    )

    volume_mkdir_ingestion_data_tropes = PythonOperator(
        task_id="volume_mkdir_ingestion_data_tropes",
        python_callable=_volume_mkdir,
        op_args=[TROPES_FOLDER],
        dag=dag
    )

    duplicate_scripts_to_volume = PythonOperator(
        task_id="duplicate_scripts_to_volume",
        python_callable=_duplicate_data_to_volume,
        dag=dag
    )

    end = EmptyOperator(task_id="end")


    # --Graph--
    volume_mkdir_ingestion_data >> [volume_mkdir_ingestion_data_scripts , volume_mkdir_ingestion_data_tropes] 
    volume_mkdir_ingestion_data_scripts >> [volume_mkdir_ingestion_data_pdf, volume_mkdir_ingestion_data_html] >> duplicate_scripts_to_volume >> end




    # Create needed folders in the volume if not existing yet.
    # Ingest local scripts (pdf & html files downloaded) to the stagging_data volume folder.
    # Ingest the trope_list in the stagging_data volume folder.
