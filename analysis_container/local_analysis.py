## Imports
import logging
from pymongo import MongoClient
import pandas as pd
import os
from random import randint

# to load the embedding model + faiss index
from sentence_transformers import SentenceTransformer
import faiss

# to use a LLM from HuggingFace 
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline
import torch

# Pyspark imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType

# logger config 
logger = logging.getLogger("local_analysis")

# volume related 
PRODUCTION_DATA_FOLDER = "./project_data/production_data"
PRODUCTION_DATA_FOLDER_LLM_OUTPUT = "./project_data/production_data/llm_ouput"


# env variables
MODEL_PATH = os.environ["MODEL_PATH"]
FAISS_INDEX_PATH = os.environ["FAISS_INDEX_PATH"]

LLM_NAME = "mistralai/Mistral-7B-Instruct-v0.2"


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



## Main functions
# Read all the scene (MongoDB connection)
def get_all_movies() : 
    _check_connection_mongoDB()
    scripts_db = _connect_mongoDB()

    movies_collection = scripts_db["movies"]

    # Note : we don't need the "full_script" field
    movies = movies_collection.find({}, {"full_script" : 0})

    
    logger.info("Data retrieval successful !")
    
    return movies


# Store all the scene in a df (or a pyspark object)
# Pyspark friendly (1 row = 1 scene of 1 movie)
def format_data (movies : list) : 
    dict_formated = {"_id":[], "movie_name":[] , "scene_content":[]}

    logger.info("Starting formating data")

    for i in range (len(movies)) : 
        for j in range (len(movies[i]["scene"])) : 
            dict_formated["_id"].append(movies[i]["_id"])
            dict_formated["movie_name"].append(movies[i]["_id"])
            dict_formated["scene_content"].append(movies[i]["scene"][j])

    df_formated = pd.DataFrame(dict_formated)

    
    logger.info("End : Formating data")

    return df_formated


@pandas_udf("double", PandasUDFType.SCALAR)
def embed_scene_content(scene_content, model_embedding):
    """
    Docstring for embed_scene_content
    
    Use of the embedding model (Bert) to embed the scene content before calculate the similarity with the faiss index

    :param scene_content: the scene content of the sliced script
    :param model_embedding: the model used to embed (Bert - the same used for tropes)
    """
    text_embedding = model_embedding.encode(scene_content)

    return text_embedding



@pandas_udf("double", PandasUDFType.SCALAR)
def similarity_calculation(text_embedding, index, tropes_db, k):
    """
    Docstring for embed_scene_content
    
    Calculation of the similarity between the embedding of scene_content and those of the trope list stored into the faiss index

    :param text_embedding: Embedding of the scene_content (see previous function)
    :param index: Faiss index - to calculate the similarity and retrieve relevant tropes 
    :param tropes_db: The tropes_db coming from MongoDB
    :param k: Maximum number of relevant tropes to retrieve.
    """
    distances, indices = index.search(text_embedding, k)

    # knowing that tropes_db is the tropes_list got from MongoDB...
    return [tropes_db[i]["name"] for i in indices[0]]


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
        temperature=0.3
    )
    response = pipe(prompt)[0]["generated_text"]

    # save the LLM response 
    i = randint(1,9)
    output_path = os.path.join(PRODUCTION_DATA_FOLDER_LLM_OUTPUT, f"test_{i}.txt")
    with open(output_path, "w") as f:
        f.write(response)
    
    
    return response


@pandas_udf("double", PandasUDFType.SCALAR)
def llm_verification(model, tokenizer, scene_content, retrieved_tropes):
    """
    Docstring for llm_verification
    
    To apply the response generation to each line of the spark_df.

    :param model: The LLM model name used for response generation
    :param tokenizer: Param for model calling.
    :param scene_content: The scene content - text format.
    :param retrieved_tropes: The tropes retrieved in this scene (with faiss index).
    """
    # call the _generate_response function
    response = _generate_response(model=model, tokenizer=tokenizer, scene_content=scene_content, retrieved_tropes=retrieved_tropes)
    return response

def format_results():
    """
    Docstring for format_results
    
    format the results (retrieved tropes + LLM response) to prepare MongoDB Maj.

    :param x: to complete 
    :param y: to complete 
    """
    pass

def main_local_analysis() : 
    
    logger.info("Local analysis started !")

    # get all the movie data
    movies = get_all_movies()

    # format the data into a dataframe pandas
    df_formated = format_data(movies=movies)

    # get the requirements : embedding model + faiss index
    model_embedding = SentenceTransformer(MODEL_PATH)
    index = faiss.read_index(FAISS_INDEX_PATH)  
    logger.info("Embedding model + faiss index retrieved successfully.")

    # Second model initialization : to check the result with a LLM    
    tokenizer = AutoTokenizer.from_pretrained(LLM_NAME)
    model = AutoModelForCausalLM.from_pretrained(LLM_NAME, torch_dtype=torch.float16, device_map="auto")


    # initialize the spark session (Use of all local core - "local[*]")
    spark = SparkSession.builder \
        .appName("PandasToSparkParallel") \
        .master("local[*]") \
        .getOrCreate()

    spark_df = spark.createDataFrame(df_formated)
    # Note : modify the defined function applied to the spark_df (incompletes)
    spark_df = spark_df.withColumn("scene_content_embeded", embed_scene_content(scene_content=spark_df["scene_content"], model_embedding=model_embedding))
    # to modify : value of k ? , and tropes_db variables (how to manage tropes retrieval ?)
    spark_df = spark_df.withColumn("similar_tropes", similarity_calculation(text_embedding=spark_df["scene_content_embeded"], index=index, tropes_db=None , k=3))
    spark_df = spark_df.withColumn("LLM_output", llm_verification(model=model, tokenizer=tokenizer, scene_content=spark_df["scene_content"], retrieved_tropes=spark_df["similar_tropes"]))

    # depending on how the last output is defined, what's next ?
    # Result formating
    # Maj of MongoDB ? 

    # ending the spark session
    spark.stop() 

    

