# Python file to be used in multiple DAGs for shared operators
import os
import pandas as pd


# To create a folder in the volume : the path must ti be already created
def _volume_mkdir(folder_path : str) : 
    # it doesn't create a folder twice if it already exist in the volume.
    os.makedirs(folder_path, exist_ok=True)


# To read the csv file as pd.Dataframe 
def _read_data_file_to_df(volume_data_folder : str, source_name : str, additional_name_component = "") :
        data_file_path = os.path.join(volume_data_folder, "data", f"{source_name}_data{additional_name_component}.csv")
        df_scrapped_data = pd.read_csv(filepath_or_buffer=data_file_path, sep=",")
        return df_scrapped_data


# To save df to csv file persistent in the Docker volume
def _save_data_file_to_csv(volume_data_folder : str, df_to_save : pd.DataFrame, destination_name : str, additional_name_component = "") :
    data_file_path = os.path.join(volume_data_folder, "data", f"{destination_name}_data{additional_name_component}.csv")
    df_to_save.to_csv(data_file_path, index=False, encoding="utf-8")
    return True