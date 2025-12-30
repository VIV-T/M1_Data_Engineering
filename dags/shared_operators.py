# Python file to be used in multiple DAGs for shared operators
import logging
import os
import pandas as pd
from pymongo import MongoClient

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



### MongoDB related operators

# Connection to MongoDB
def _connect_mongoDB() : 
        client = MongoClient(
            host= f"mongodb://mongo:27017/",
            username="admin",
            password="admin"
        )
        logging.info("MongoDB client created")
        logging.info(f"Databases available: {client.list_database_names()}") 
        scripts_db = client["scriptsDB"]
        return scripts_db


# check connection to MongoDB
def _check_connection_mongoDB() :
    try :
        _connect_mongoDB()
        logging.info("Success connecting to MongoDB")
    except Exception as e:
        logging.error(f"Error connecting to MongoDB: {e}")
        exit(1)


# create a collection in MongoDB
def _create_collection(collection_name : str) :
    scripts_db = _connect_mongoDB()
    # check if the collection already exists
    logging.info(f"{scripts_db.list_collection_names()}")

    # logger.info(f"{scripts_db.list_collection_names()}")
    try : 
        if not collection_name in scripts_db.list_collection_names() :   
            scripts_collection = scripts_db[f"{collection_name}"]
            logging.info("Success creating the collection in MongoDB")
        else : 
            logging.info("The collection already exists in MongoDB")
    except Exception as e:
        logging.error(f"Error creating the collection in MongoDB: {e}")