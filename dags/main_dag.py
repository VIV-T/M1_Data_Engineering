### Imports
import logging
import pendulum

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
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


    # --Task--
    launch_scrapping = DockerOperator(
        task_id='launch_scrapping_container',
        image='scrapping_container:latest',  # Assurez-vous que cette image est construite
        command="python /app/scrapping/scrapping_dag.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        auto_remove='success',
        dag=dag
    )

    # --Graph--
    launch_scrapping