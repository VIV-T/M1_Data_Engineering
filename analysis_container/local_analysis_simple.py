## Imports
import logging
from pymongo import MongoClient
import pandas as pd
import os
from random import randint
import traceback

# to load the embedding model + faiss index
from sentence_transformers import SentenceTransformer
import faiss

# to use a LLM from HuggingFace
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline
import torch


# logger config
logger = logging.getLogger("local_analysis")

# volume related
PRODUCTION_DATA_FOLDER = "./project_data/production_data"


# env variables
MODEL_NAME = os.environ["MODEL_NAME"]
MODEL_PATH = os.path.join(PRODUCTION_DATA_FOLDER, "tools", MODEL_NAME)
FAISS_INDEX_NAME = os.environ["FAISS_INDEX_NAME"]
FAISS_INDEX_PATH = os.path.join(PRODUCTION_DATA_FOLDER, "tools", FAISS_INDEX_NAME)

LLM_NAME = "mistralai/Mistral-7B-Instruct-v0.2"

# Lazy-loaded globals for worker processes
_MODEL_EMBEDDING = None
_FAISS_INDEX = None
_TROPES_LIST = None
_K_NEIGHBORS = 5


## MongoDB tools
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


def _update_movie_tropes(movie_name, tropes_list) :
    """
    Docstring for _update_movie_tropes

    Update the movie document in MongoDB with the retrieved tropes.

    :param movie_name: The name of the movie to update.
    :param tropes_list: The list of tropes to add to the movie document.
    """
    try :
        scripts_db = _connect_mongoDB()
        movies_collection = scripts_db["movies"]

        result = movies_collection.update_one(
            {"name": movie_name},
            {"$set": {"tropes": tropes_list}}
        )

        if result.modified_count == 1 :
            logger.info(f"Movie {movie_name} updated successfully with tropes.")
        else :
            logger.warning(f"No update made for movie {movie_name}. It may not exist.")
    
    except Exception as e:
        logger.error(f"Error updating movie {movie_name} in MongoDB: {e}")
        traceback.print_exc()


## Main functions
# Read all the scene (MongoDB connection)
def get_all_movies() :
    _check_connection_mongoDB()
    scripts_db = _connect_mongoDB()

    movies_collection = scripts_db["movies"]

    # Note : we don't need the "full_script" field
    # be careful if the dataset is too large to fit into memory : here it's ok.
    movies = list(movies_collection.find({}, {"full_script" : 0}))


    logger.info("Movies data retrieval successful !")

    return movies


def get_all_tropes():
    _check_connection_mongoDB()
    scripts_db = _connect_mongoDB()

    tropes_collection = scripts_db["tropes"]

    # Note : we don't need the "full_script" field
    tropes = tropes_collection.find({})


    logger.info("Tropes data retrieval successful !")

    return tropes

# Store all the scene in a df (or a pyspark object)
# Pyspark friendly (1 row = 1 scene of 1 movie)
def format_data (movies : list) :
    dict_formated = {"movie_name":[] , "scene_content":[]}

    logger.info("Starting formating data")

    for i in range (len(movies)) :
        for j in range (len(movies[i]["scenes"])) :
            dict_formated["movie_name"].append(movies[i]["name"])
            dict_formated["scene_content"].append(movies[i]["scenes"][j])

    df_formated = pd.DataFrame(dict_formated)

    logger.info("End : Formating data")

    return df_formated


def _embed_scene_content(scene_content):
    """
    Docstring for embed_scene_content

    Use of the embedding model (Bert) to embed the scene content before calculate the similarity with the faiss index

    :param scene_content: the scene content of the sliced script
    :param model_embedding: the model used to embed (Bert - the same used for tropes)
    """
    logger.info("Embedding scene content...")
    return _MODEL_EMBEDDING.encode(scene_content)


def _similarity_calculation(text_embedding):
    """
    Docstring for embed_scene_content

    Calculation of the similarity between the embedding of scene_content and those of the trope list stored into the faiss index

    :param text_embedding: Embedding of the scene_content (see previous function)
    :param index: Faiss index - to calculate the similarity and retrieve relevant tropes
    :param tropes_db: The tropes_db coming from MongoDB
    :param k: Maximum number of relevant tropes to retrieve.
    """
    distances, indices = _FAISS_INDEX.search(text_embedding, _K_NEIGHBORS)
    results = []
    for index_row in indices:
        results.append([_TROPES_LIST[i]["name"] for i in index_row])
    return results



def _generate_response(model, tokenizer, scene_content, retrieved_tropes):
    """
    Docstring for _generate_response

    Call fo a LLM model to check if the tropes retrieval is correct.

    :param model: The LLM model name used for response generation
    :param tokenizer: Param for model calling.
    :param scene_content: The scene content - text format.
    :param retrieved_tropes: The tropes retrieved in this scene (with faiss index).
    """
    task = "Analyze this **movie scene script** and confirm the presence of the following **tropes**."

    prompt = f"""
    {task}
    **Movie scene script** : {scene_content}

    Potential **tropes** : {', '.join(retrieved_tropes)}

   **Instructions** :
    1. Confirm if those tropes are present in the movie scene (be critical)
    2. If yes, give an example based on the **movie scene script**.
    3. Be critical, don't hesitate to answer with an empty string.

    **Expected output**
    a. If the tropes aren't relevant, answer with an **empty string**.
    b. If you got no tropes (empty variable), answer with an **empty string**.
    c. Else, answer with **JSON** : {{"tropes": ["trope1", "trope2"], "examples": ["...", "..."]}}.
    """

    pipe = pipeline(
        "text-generation",
        model=model,
        tokenizer=tokenizer,
        max_new_tokens=256,
        temperature=0
    )
    response = pipe(prompt)[0]["generated_text"]

    return response[len(prompt):].strip()



def _llm_verification(model, tokenizer, scene_content, retrieved_tropes):
    """
    Docstring for llm_verification

    Applies LLM verification to each scene to validate if the retrieved tropes 
    are actually present in the text.

    :param model: The LLM model name used for response generation
    :param tokenizer: Param for model calling.
    :param scene_content: The scene content - text format.
    :param retrieved_tropes: The tropes retrieved in this scene (with faiss index).
    """
    results = []
    for content, tropes in zip(scene_content, retrieved_tropes):
        try:
            response = _generate_response(
                model=model, 
                tokenizer=tokenizer, 
                scene_content=content, 
                retrieved_tropes=tropes
            )
            # Simple scoring: count of confirmed tropes in the response
            score = sum(1 for trope in tropes if trope in response)
            results.append(float(score))
        except Exception as e:
            results.append(0.0)

    return pd.Series(results)

def _unique_tropes(trope_list):
    """
    Docstring for unique_tropes

    get the unique tropes from a list of tropes (with possible duplicates)

    :param trope_lists: list of list of tropes
    """
    return list(set().union(*trope_list))

def _format_results(df_to_format):
    """
    Docstring for format_results

    format the results (retrieved tropes + LLM response) to prepare MongoDB Maj.

    :param x: to complete
    :param y: to complete
    """
    result = (
        df_to_format.groupby('movie_name')['retrieved_tropes']
        .agg(_unique_tropes)
        .reset_index()
    )
    return result

def main_local_analysis() :

    logger.info("Local analysis started !")

    # get all the movie data & tropes data
    movies = get_all_movies()
    tropes = get_all_tropes()
    global _TROPES_LIST
    _TROPES_LIST = list(tropes)

    # format the data into a dataframe pandas
    df_data = format_data(movies=movies)

    
    output_csv_path = os.path.join(PRODUCTION_DATA_FOLDER, "local_analysis_tests.csv")
    df_data.to_csv(output_csv_path, index=False)

    # get the requirements : embedding model + faiss index
    global _MODEL_EMBEDDING
    _MODEL_EMBEDDING = SentenceTransformer(MODEL_PATH)
    logger.info("Embedding model retrieved successfully.")

    global _FAISS_INDEX
    _FAISS_INDEX = faiss.read_index(FAISS_INDEX_PATH)
    logger.info("Faiss index retrieved successfully.")


    scene_contents = df_data["scene_content"].tolist()
    embeddings = _embed_scene_content(scene_contents)
    logger.info("Scene embedding calculation done.")
    df_data["retrieved_tropes"] = _similarity_calculation(embeddings)
    logger.info("Similarity calculation done.")

    # LLM related - hardware limitations
    # Second model initialization : to check the result with a LLM
    #_TOKENIZER = AutoTokenizer.from_pretrained(LLM_NAME)
    #logger.info("LLM tokenizer initialized.")
    #_MODEL = AutoModelForCausalLM.from_pretrained(LLM_NAME, torch_dtype=torch.float16, device_map="auto")
    #logger.info("LLM model initialized.")
    # df_formated["llm_check"] = _llm_verification(
    #     model=_MODEL,
    #     tokenizer=_TOKENIZER,
    #     scene_content=df_formated["scene_content"],
    #     retrieved_tropes=df_formated["retrieved_tropes"]
    # )
    # logger.info("LLM verification done.")
    

    # store the results into a csv file

    df_formated = _format_results(df_to_format=df_data)

    
    output_csv_path = os.path.join(PRODUCTION_DATA_FOLDER, "local_analysis_results.csv")
    df_formated.to_csv(output_csv_path, index=False)

    
    # MongoDB update
    for _, row in df_formated.iterrows():
        try:
            movie_name = row["movie_name"]
            tropes_list = row["retrieved_tropes"]

            _update_movie_tropes(movie_name, tropes_list)

        except Exception as e:
            logger.error(f"Failed to update movie {row.get('movie_name', 'Unknown')}: {e}")
    
    logger.info("MongoDB update process completed.")
    logger.info("Local analysis completed !")



