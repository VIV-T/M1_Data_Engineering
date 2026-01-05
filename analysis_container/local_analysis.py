## Imports
import logging
from pymongo import MongoClient
import pandas as pd
import os
from sentence_transformers import SentenceTransformer
import faiss


from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline
import torch


from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType

# logger config 
logger = logging.getLogger("local_analysis")

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
def embed_scene_content(v, model_embedding):
    """
    Docstring for embed_scene_content
    
    Use of the embedding model (Bert) to embed the scene content before calculate the similarity with the faiss index

    :param v: Description
    """
    text_embedding = model_embedding.encode(v)

    return text_embedding



@pandas_udf("double", PandasUDFType.SCALAR)
def similarity_calculation(text_embedding, index, tropes_db, k):
    """
    Docstring for embed_scene_content
    
    Calculation of the similarity between the embedding of scene_content and those of the trope list stored into the faiss index

    :param v: Description
    """
    distances, indices = index.search(text_embedding, k)

    # knowing that tropes_db is the tropes_list got from MongoDB...
    return [tropes_db[i]["name"] for i in indices[0]]


def generate_response(model, tokenizer, text, retrieved_tropes, is_global=False):
    if is_global:
        task = "Analyse ce script **dans son ensemble** et confirme la présence des tropes suivants."
    else:
        task = "Analyse ce **segment de script** et confirme la présence des tropes suivants."

    prompt = f"""
    {task}
    Texte : {text}

    Tropes potentiels : {', '.join(retrieved_tropes)}

    **Consignes** :
    1. Confirme si ces tropes sont présents.
    2. Si oui, explique pourquoi.
    3. Si non, propose d'autres tropes pertinents.
    4. Réponds en JSON : {{"tropes": ["trope1", "trope2"], "explications": "..."}}.
    """

    pipe = pipeline(
        "text-generation",
        model=model,
        tokenizer=tokenizer,
        max_new_tokens=256,
        temperature=0.3
    )
    response = pipe(prompt)[0]["generated_text"]
    return response


@pandas_udf("double", PandasUDFType.SCALAR)
def llm_verification(v):
    """
    Docstring for embed_scene_content
    
    Calculation of the similarity between the embedding of scene_content and those of the trope list stored into the faiss index

    :param v: Description
    """
    # call the generate_response function
    return v * 2



def main_local_analysis() : 
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
    spark_df = spark_df.withColumn("scene_content_embeded", embed_scene_content(spark_df["valeur"]))
    spark_df = spark_df.withColumn("similar_tropes", similarity_calculation(spark_df["valeur"]))
    spark_df = spark_df.withColumn("similar_tropes", llm_verification(spark_df["valeur"]))

    # depending on how the last output is defined, what's next ?
    # Maj of MongoDB ? 

    # ending the spark session
    spark.stop() 

    


# use a "map" with Pyspark to apply similarity calculation after loading the model to embed each scene content (also with a "map") and load the faiss index.