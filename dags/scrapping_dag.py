### Imports
import logging
import pendulum
from docker.types import Mount
import os
import pandas as pd
import re
import numpy as np

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import timedelta

from shared_operators import _read_data_file_to_df, _save_data_file_to_csv

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
    dag_id="scrapping_dag",
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
    # To read the csv file as pd.Dataframe 
    # def _read_data_file_to_df(source_name : str, additional_name_component = "") :
    #     data_file_path = os.path.join(SCRAPPING_DATA_FOLDER, "data", f"{source_name}_data{additional_name_component}.csv")
    #     df_scrapped_data = pd.read_csv(filepath_or_buffer=data_file_path, sep=",")
    #     return df_scrapped_data
    

    # To save df to csv file persistent in the Docker volume
    # def _save_data_file_to_csv(df_to_save : pd.DataFrame, destination_name : str, additional_name_component = "") :
    #     data_file_path = os.path.join(SCRAPPING_DATA_FOLDER, "data", f"{destination_name}_data{additional_name_component}.csv")
    #     df_to_save.to_csv(data_file_path, index=False, encoding="utf-8")
    #     return True


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
        df_scrapped_data = _read_data_file_to_df(volume_data_folder=SCRAPPING_DATA_FOLDER, source_name=source_name)
        df_scrapped_data[f"movie_name_{source_name}_conventioned"] = df_scrapped_data[f"movie_name_{source_name}"].apply(_apply_naming_convention)
        df_scrapped_data_modified = df_scrapped_data.drop(f"movie_name_{source_name}", axis=1)
        
        saved = False
        while not saved == True :
            saved =_save_data_file_to_csv(volume_data_folder=SCRAPPING_DATA_FOLDER, df_to_save=df_scrapped_data_modified, destination_name=source_name, additional_name_component="_to_merge")
        return True



    def _merge_and_transform_scrapped_data(source_1 : str, source_2 : str, destination_name : str):
        df_scrapped_data_source_1 = _read_data_file_to_df(volume_data_folder=SCRAPPING_DATA_FOLDER, source_name=source_1, additional_name_component="_to_merge")
        df_scrapped_data_source_2 = _read_data_file_to_df(volume_data_folder=SCRAPPING_DATA_FOLDER,source_name=source_2, additional_name_component="_to_merge")
        destination_name = destination_name

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
        df_scrapped_data_merged[f"{destination_name}_movie_name_conventioned"] = np.where(
            df_scrapped_data_merged[f"merge_{source_1}_{source_2}"] != "right_only", 
            df_scrapped_data_merged[f"movie_name_{source_1}_conventioned"], 
            df_scrapped_data_merged[f"movie_name_{source_2}_conventioned"]
        )

        # 2. one unique column for url (pdf & html) : based on priority on scipts_slug data, and then completed by imsdb data.
        df_scrapped_data_merged[f"{destination_name}_url"] = np.where(
            df_scrapped_data_merged[f"merge_{source_1}_{source_2}"] != "right_only", 
            df_scrapped_data_merged[f"url_{source_1}"], 
            df_scrapped_data_merged[f"url_{source_2}"]
        )

        # 3. create the url_extension column useful for scrapping
        df_scrapped_data_merged[f"{destination_name}_url_extension"] = np.where(
            df_scrapped_data_merged[f"merge_{source_1}_{source_2}"] != "right_only", 
            'pdf', 
            'html')

        # 4. Drop the useless columns of the final dataframe
        df_scrapped_data_merged = df_scrapped_data_merged.drop(
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
            saved = _save_data_file_to_csv(volume_data_folder=SCRAPPING_DATA_FOLDER, df_to_save=df_scrapped_data_merged, destination_name=destination_name, additional_name_component="")
        return True


    # To create a column in the df containing the file name
    def _filename_column_creation(source_name, destination_name) :
        # create the "destination_name"
        destination_name = destination_name

        # read the file previously created.
        df_merged_scrapping_data = _read_data_file_to_df(volume_data_folder=SCRAPPING_DATA_FOLDER, source_name=source_name)

        # filename creation
        filename_list = []
        for index, row in df_merged_scrapping_data.iterrows():
            name = row[f"{source_name}_movie_name_conventioned"]
            name = name.lower()
            name = name.replace(" ", "_")
            row = name + "." + row[f"{source_name}_url_extension"]
            filename_list.append(row)

        df_merged_scrapping_data[f"{source_name}_filename"] = filename_list

        # Change the column name to fit the new filename (destination_name) : 
        for column in df_merged_scrapping_data : 
            new_column = column.replace(source_name, destination_name)
            # inplace = True allow us to modify the df instead of creating a copy.
            df_merged_scrapping_data.rename(columns={column : new_column}, inplace=True) 

            
        # Save this new dataframe
        saved = False 
        while not saved == True :
            saved = _save_data_file_to_csv(volume_data_folder=SCRAPPING_DATA_FOLDER, df_to_save=df_merged_scrapping_data, destination_name=destination_name, additional_name_component="")
        return True

    ### --Task--
    # Lauch the scrapping container to fetch the data and save it into a csv file in the Docker volume
    launch_scrapper_container = DockerOperator(
        task_id='launch_scrapping_container',
        container_name="scrapper_container",
        image='m1_data_engineering-scrapper:latest',   # use the docker image build by the 'scrapper' service in the docker-compose.yml
        command=["/opt/venv_scrapping/bin/python", "/app/scrapping_container/scrapping_files/main.py"],
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



    # To apply naming convention to all movie name => to be able to merge data properly
    apply_naming_convention_to_script_slug_data = PythonOperator(
        task_id="apply_naming_convention_to_script_slug_data",
        python_callable=_apply_naming_convention_to_scrapped_data,
        op_args=["script_slug"],
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
        op_args=["script_slug", "imsdb", "merged_scrapping"],
        dag=dag
    )

    filename_column_creation = PythonOperator(
        task_id="filename_column_creation",
        python_callable=_filename_column_creation,
        op_args=["merged_scrapping", "scrapping"],
        dag=dag
    )


    ### --Graph--
    launch_scrapper_container >> [apply_naming_convention_to_script_slug_data, apply_naming_convention_to_imsdb_data] >> \
    merge_scrapped_data >> filename_column_creation