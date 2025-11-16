### Imports
import logging
import pendulum
from docker.types import Mount
import os
import glob
import pandas as pd
import re
import numpy as np

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
VOLUME_FOLDER = os.path.join("/opt", "airflow", "data_scrapping")

with DAG(
    dag_id="ingestion_dag_bis",
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
    #logger.info(os.getcwd())

    # To read the csv file as pd.Dataframe 
    def _read_data_file_to_df(source_name : str, additional_name_component = "") :
        data_file_path = os.path.join(VOLUME_FOLDER, "data", f"{source_name}_data{additional_name_component}.csv")
        df_scrapped_data = pd.read_csv(filepath_or_buffer=data_file_path, sep=",")
        return df_scrapped_data
    

    # To save df to csv file persistent in the Docker volume
    def _save_data_file_to_csv(df_to_save : pd.DataFrame, source_name : str, additional_name_component = "") :
        data_file_path = os.path.join(VOLUME_FOLDER, "data", f"{source_name}_data{additional_name_component}.csv")
        df_to_save.to_csv(data_file_path, index=False, encoding="utf-8")
        return True


    # The function to apply naming convention
    def _apply_naming_convention(string) :
        string = string.replace(",", "")
        string = string.replace(":", "")
        string = string.replace(";", "")
        string = string.replace("'", "")
        string = string.replace(".", "")
        string = string.replace("!", "")
        string = string.replace("?", "")
        string = string.replace("IX", "9")
        string = string.replace("VIII", "8")
        string = string.replace("VII", "7")
        string = string.replace("VI", "6")
        string = string.replace("IV", "4")
        string = string.replace("III", "3")
        string = string.replace("II", "2")
        words = re.split(r"[\s_-]+", string)
        
        new_string = ' '.join(word.capitalize() for word in words)

        return new_string


    # read data file, apply naming convention, save the new file
    def _apply_naming_convention_to_scrapped_data(source_name : str):
        df_scrapped_data = _read_data_file_to_df(source_name=source_name)
        df_scrapped_data[f"movie_name_{source_name}_conventioned"] = df_scrapped_data[f"movie_name_{source_name}"].apply(_apply_naming_convention)
        df_scrapped_data_modified = df_scrapped_data.drop(f"movie_name_{source_name}", axis=1)
        
        saved = False
        while not saved == True :
            saved =_save_data_file_to_csv(df_to_save=df_scrapped_data_modified, source_name=source_name, additional_name_component="_to_merge")
        return True



    def _merge_and_transform_scrapped_data(source_1 : str, source_2 : str):
        df_scrapped_data_source_1 = _read_data_file_to_df(source_name=source_1, additional_name_component="_to_merge")
        df_scrapped_data_source_2 = _read_data_file_to_df(source_name=source_2, additional_name_component="_to_merge")

        ## Merge
        df_scrapped_data_merged = pd.merge(
            df_scrapped_data_source_1,
            df_scrapped_data_source_2, 
            left_on=f"movie_name_{source_1}_conventioned", 
            right_on=f"movie_name_{source_2}_conventioned", 
            how="outer", 
            indicator=f"merge_{source_1}_{source_2}"
        ) 

        ## Transform
        # 1. one unique column for movie_name : based on priority on scipts_slug data, and then completed by imsdb data.
        df_scrapped_data_merged["movie_name_conventioned"] = np.where(
            df_scrapped_data_merged[f"merge_{source_1}_{source_2}"] != "right_only", 
            df_scrapped_data_merged[f"movie_name_{source_1}_conventioned"], 
            df_scrapped_data_merged[f"movie_name_{source_2}_conventioned"]
        )

        # 2. one unique column for url (pdf & html) : based on priority on scipts_slug data, and then completed by imsdb data.
        df_scrapped_data_merged["scrapping_url"] = np.where(
            df_scrapped_data_merged[f"merge_{source_1}_{source_2}"] != "right_only", 
            df_scrapped_data_merged[f"url_{source_1}"], 
            df_scrapped_data_merged[f"url_{source_2}"]
        )

        # 3. create the url_extension column useful for scrapping
        df_scrapped_data_merged['scrapping_url_extension'] = np.where(
            df_scrapped_data_merged[f"merge_{source_1}_{source_2}"] != "right_only", 
            'pdf', 
            'html')

        # 4. Drop the useless columns of the final dataframe
        df_scrapped_data_merged_final = df_scrapped_data_merged.drop(
                                                        [
                                                            f"movie_name_{source_1}_conventioned", 
                                                            f"movie_name_{source_2}_conventioned", 
                                                            f"merge_{source_1}_{source_2}", 
                                                            f"url_{source_1}", 
                                                            f"url_{source_2}"
                                                        ], 
                                                        axis=1)

        # 5. Save this new dataframe
        saved = False 
        while not saved == True :
            saved = _save_data_file_to_csv(df_to_save=df_scrapped_data_merged_final, source_name="downloadable_url", additional_name_component="")
        return True



    ### --Task--
    # To apply naming convention to all movie name => to be able to merge data properly
    apply_naming_convention_to_scripts_slug_data = PythonOperator(
        task_id="apply_naming_convention_to_scripts_slug_data",
        python_callable=_apply_naming_convention_to_scrapped_data,
        op_args=["scripts_slug"],
        dag=dag
    )
    
    apply_naming_convention_to_imsdb_data = PythonOperator(
        task_id="apply_naming_convention_to_imsdb_data",
        python_callable=_apply_naming_convention_to_scrapped_data,
        op_args=["imsdb"],
        dag=dag
    )


    # merge the data based on the movie_name key (respecting naming convention)
    # output : a nex csv file cleaned (will be used to download the ressources)
    merge_scrapped_data = PythonOperator(
        task_id="merge_scrapped_data",
        python_callable=_merge_and_transform_scrapped_data,
        op_args=["scripts_slug", "imsdb"],
        dag=dag
    )


    ### --Graph--
    [apply_naming_convention_to_scripts_slug_data, apply_naming_convention_to_imsdb_data] >> merge_scrapped_data