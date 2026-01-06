### Imports
import logging
import pendulum
from docker.types import Mount
import os
import pandas as pd
import re
import numpy as np
import requests
from bs4 import BeautifulSoup

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
    template_searchpath=["/opt/airflow/data/"],
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
        df_scrapped_data_merged["movie_name_conventioned"] = np.where(
            df_scrapped_data_merged[f"merge_{source_1}_{source_2}"] != "right_only", 
            df_scrapped_data_merged[f"movie_name_{source_1}_conventioned"], 
            df_scrapped_data_merged[f"movie_name_{source_2}_conventioned"]
        )

        # 2. one unique column for url (pdf & html) : based on priority on scipts_slug data, and then completed by imsdb data.
        df_scrapped_data_merged["url"] = np.where(
            df_scrapped_data_merged[f"merge_{source_1}_{source_2}"] != "right_only", 
            df_scrapped_data_merged[f"url_{source_1}"], 
            df_scrapped_data_merged[f"url_{source_2}"]
        )

        # 3. create the url_extension column useful for scrapping
        df_scrapped_data_merged["url_extension"] = np.where(
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
            name = row["movie_name_conventioned"]
            name = name.lower()
            name = name.replace(" ", "_")
            row = name + "." + row["url_extension"]
            filename_list.append(row)

        df_merged_scrapping_data["filename"] = filename_list

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

    
    
    ## TROPES Part.
    def _to_tropedia_format(title):
        """
        Format a movie title to a Tropedia url format.
        """
        logging.info(f"Formatting title: {title}")
        return title.strip().replace(" ", "_").replace(":", "")
    

    def _apply_formatting(volume_data_folder, source_name, destination_name):
        """
        Apply Tropedia formatting to the title column.
        Add 'url' column, with Tropedia url of the movie.
        Add 'exists_on_tropedia' columns and set False by default.
        """

        base_url = "https://tropedia.fandom.com/wiki/"

        logging.info(f"Starting apply_formatting step")

        df = _read_data_file_to_df(volume_data_folder=volume_data_folder, source_name=source_name)

        df["tropedia_name"] = df["movie_name_conventioned"].apply(_to_tropedia_format) # Apply naming convention
        df["url_tropedia"] = base_url + df["tropedia_name"] # Create the full url
        
        # Save this new dataframe
        saved = False 
        while not saved == True :
            saved =_save_data_file_to_csv(volume_data_folder=volume_data_folder, destination_name=destination_name, df_to_save=df)

        logging.info(f"Finished apply_formatting step")
        return True


    def _is_on_tropedia(volume_data_folder, source_name, destination_name):
        """
        Check if the Tropedia page exists for each movie and update the 'is_on_tropedia' column.
        A page is considered non-existent if it contains the text "There is currently no text in this page".
        """
    
        logging.info(f"Starting is_on_tropedia step")

        df = _read_data_file_to_df(volume_data_folder=volume_data_folder, source_name=source_name)
        #df = df.head(50)  # Limit to first 50 rows for testing purposes

        for i, row in df.iterrows():
            try:
                logger.info(f"Checking URL {i+1}/{len(df)}: {row['url_tropedia']}")
                response = requests.get(row["url_tropedia"], timeout=10) # Ajout d'un timeout
                
                # Check if the page is valid or if the content is empty
                soup = BeautifulSoup(response.text, "html.parser")
                p = soup.select_one("#mw-content-text > div > p")
                
                if p and "There is currently no text in this page" in p.text:
                    df.at[i, "is_on_tropedia"] = False
                else:
                    df.at[i, "is_on_tropedia"] = True

            except Exception as e:
                logger.error(f"Error in checking url : {row['url']}")

        # Save this new dataframe
        saved = False 
        while not saved == True :
            saved = _save_data_file_to_csv(volume_data_folder=volume_data_folder, destination_name=destination_name, df_to_save=df)

        logging.info(f"Finished is_on_tropedia step, the column 'exists_on_tropedia' has been updated.")
        return True




    def _clean_csv(volume_data_folder, source_name, destination_name_scripts, destination_name_tropes): # path_input_file, output_file_scripts, output_file_tropes
        """Filter the CSV to keep only the movies that exist on Tropedia and drop unnecessary columns.
           The first output file is for scripts data, the second for tropes data.
        """
        logging.info(f"Starting cleaning csv file")
        logging.info(f"Reading source file: {source_name}")
        logging.info(f"Output scripts file: {destination_name_scripts}")
        logging.info(f"Output tropes file: {destination_name_tropes}")

        df = _read_data_file_to_df(volume_data_folder=volume_data_folder, source_name=source_name)

        # For scripts data
        df_scripts = df[df["is_on_tropedia"] == True]
        df_scripts = df_scripts.drop(columns=["tropedia_name", "is_on_tropedia", "url_tropedia"])
        # Save this new dataframe
        scripts_saved = False 
        while not scripts_saved == True :
            scripts_saved = _save_data_file_to_csv(volume_data_folder=volume_data_folder, destination_name=destination_name_scripts, df_to_save=df_scripts)

        logging.info("List of scripts movies that exist on Tropedia saved for SCRIPTS part.")
    

        # For tropes data
        df_tropes = df[df["is_on_tropedia"] == True]
        df_tropes = df_tropes.drop(columns=["tropedia_name", "scrapping_url", "scrapping_url_extension"]) # WARNING : REMOVE THE COLUMN 
        # Save this new dataframe
        tropes_saved = False 
        while not tropes_saved == True :
            tropes_saved = _save_data_file_to_csv(volume_data_folder=volume_data_folder, destination_name=destination_name_tropes, df_to_save=df_tropes)

        logging.info("List of scripts movies that exist on Tropedia saved for TROPES part.")

        return True



    ### --Task--
    ## Scripts tasks
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
        # run the scrapper container as root so it can change ownership; pass AIRFLOW_UID so the container
        # can chown files back to the Airflow user
        user="root",
        environment={"AIRFLOW_UID": os.environ.get("AIRFLOW_UID", "50000")}, # useful for permission management

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

    ## Tropes tasks
    apply_formatting = PythonOperator(
        task_id="apply_formatting",
        python_callable=_apply_formatting,
        # Python args : 
        # input_volume_data_folder, *
        # source_name, 
        # output_volume_data_folder, 
        # destination_name
        op_args=[
            SCRAPPING_DATA_FOLDER, 
            "scrapping", 
            "formated_scrapping"
            ], 
        dag=dag
    )


    is_on_tropedia = PythonOperator(
        task_id="is_on_tropedia",
        python_callable=_is_on_tropedia,
        # Python args : 
        # volume_data_folder (the same for the input and the output here)
        # source_name
        # destination_name
        op_args=[
            SCRAPPING_DATA_FOLDER, 
            "formated_scrapping", 
            "is_on_tropedia"
        ],
        dag=dag
    )

    clean_csv = PythonOperator(
        task_id="clean_csv",
        python_callable=_clean_csv,
        # Python args 
        # volume_data_folder, 
        # source_name, 
        # destination_name_scripts, 
        # destination_name_tropes
        # Nt : input = movies list of scripts with tropedia atributes , output 1 = scripts data , output 2 = tropedia data
        op_args=[
            SCRAPPING_DATA_FOLDER, 
            "is_on_tropedia", 
            "ingestion_scripts", 
            "ingestion_tropes"
        ], 
        dag=dag
    )

    end = EmptyOperator(task_id="end")


    ### --Graph--
    launch_scrapper_container >> \
    [apply_naming_convention_to_script_slug_data, apply_naming_convention_to_imsdb_data] >> \
    merge_scrapped_data >> filename_column_creation >> \
    apply_formatting >> is_on_tropedia >> clean_csv >> end
