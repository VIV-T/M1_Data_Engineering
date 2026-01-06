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
from pyspark.sql.types import ArrayType, FloatType, StringType

# logger config
logger = logging.getLogger("local_analysis")

# volume related
PRODUCTION_DATA_FOLDER = "./project_data/production_data"
PRODUCTION_DATA_FOLDER_LLM_OUTPUT = "./project_data/production_data/llm_ouput"


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
_K_NEIGHBORS = 3


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
    dict_formated = {"_id":[], "movie_name":[] , "scene_content":[]}

    logger.info("Starting formating data")

    for i in range (len(movies)) :
        for j in range (len(movies[i]["scenes"])) :
            dict_formated["_id"].append(str(movies[i]["_id"]))
            dict_formated["movie_name"].append(movies[i]["name"])
            dict_formated["scene_content"].append(movies[i]["scenes"][j])

    df_formated = pd.DataFrame(dict_formated)


    logger.info("End : Formating data")

    return df_formated



# def _generate_response(model, tokenizer, scene_content, retrieved_tropes):
#     """
#     Docstring for _generate_response

#     Call fo a LLM model to check if the tropes retrieval is correct.

#     :param model: The LLM model name used for response generation
#     :param tokenizer: Param for model calling.
#     :param scene_content: The scene content - text format.
#     :param retrieved_tropes: The tropes retrieved in this scene (with faiss index).
#     """
#     task = "Analyze this **movie scene script** and confirm the presence of the following **tropes**."

#     prompt = f"""
#     {task}
#     **Movie scene script** : {scene_content}

#     Potential **tropes** : {', '.join(retrieved_tropes)}

#     **Instructions** :
#     1. Confirm if those tropes are present in the movie scene (be critical)
#     2. If yes, give an example based on the **movie scene script**.
#     3. Be critical, don't hesitate to answer with an empty string.

#     **Expected output**
#     a. If the tropes aren't relevant, answer with an **empty string**.
#     b. If you got no tropes (empty variable), answer with an **empty string**.
#     c. Else, answer with **JSON** : {{"tropes": ["trope1", "trope2"], "examples": ["...", "..."]}}.
#     """

#     pipe = pipeline(
#         "text-generation",
#         model=model,
#         tokenizer=tokenizer,
#         max_new_tokens=256,
#         temperature=0
#     )
#     response = pipe(prompt)[0]["generated_text"]

#     # save the LLM response
#     i = randint(1,9)
#     output_path = os.path.join(PRODUCTION_DATA_FOLDER_LLM_OUTPUT, f"test_{i}.txt")
#     with open(output_path, "w") as f:
#         f.write(response)


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

    # get all the movie data & tropes data
    movies = get_all_movies()
    tropes = get_all_tropes()
    _TROPES_LIST = list(tropes)

    # format the data into a dataframe pandas
    df_formated = format_data(movies=movies)

    # get the requirements : embedding model + faiss index
    _MODEL_EMBEDDING = SentenceTransformer(MODEL_PATH)
    logger.info("Embedding model retrieved successfully.")
    _FAISS_INDEX = faiss.read_index(FAISS_INDEX_PATH)
    logger.info("Faiss index retrieved successfully.")

    # Second model initialization : to check the result with a LLM
    #tokenizer = AutoTokenizer.from_pretrained(LLM_NAME)
    #model = AutoModelForCausalLM.from_pretrained(LLM_NAME, torch_dtype=torch.float16, device_map="auto")

    #logger.info("LLM model and tokenizer initialized.")

    # initialize the spark session (Use of all local core - "local[*]")
    spark = SparkSession.builder \
        .appName("PandasToSparkParallel") \
        .master("local[*]") \
        .getOrCreate()

    logger.info("Spark Session initialized")
    
    # Make sure the current module is available to Spark worker processes
    try:
        spark.sparkContext.addPyFile(__file__)
        logger.info(f"Added {__file__} to Spark python files")
    except Exception:
        logger.warning("Could not add local module file to Spark executors; proceeding anyway")

    spark_df = spark.createDataFrame(df_formated)

    logger.info("Spark DataFrame created from the pandas DataFrame.")

    ## Pandas UDF definition - after the Pyspark session initialization.
    # The pandas UDF needs an active PySpark session to work
    @pandas_udf(ArrayType(FloatType()))
    def embed_scene_content(scene_content):
        """
        Docstring for embed_scene_content

        Use of the embedding model (Bert) to embed the scene content before calculate the similarity with the faiss index

        :param scene_content: the scene content of the sliced script
        :param model_embedding: the model used to embed (Bert - the same used for tropes)
        """
        global _MODEL_EMBEDDING
        if _MODEL_EMBEDDING is None:
            # lazy load inside worker
            _MODEL_EMBEDDING = SentenceTransformer(MODEL_PATH)
            
            logger.info("Embedding model retrieved successfully.")

        texts = scene_content.tolist()
        embeddings = _MODEL_EMBEDDING.encode(texts, convert_to_numpy=True)
        return pd.Series([emb.tolist() for emb in embeddings])



    @pandas_udf(ArrayType(StringType()))
    def similarity_calculation(text_embedding):
        """
        Docstring for embed_scene_content

        Calculation of the similarity between the embedding of scene_content and those of the trope list stored into the faiss index

        :param text_embedding: Embedding of the scene_content (see previous function)
        :param index: Faiss index - to calculate the similarity and retrieve relevant tropes
        :param tropes_db: The tropes_db coming from MongoDB
        :param k: Maximum number of relevant tropes to retrieve.
        """
        global _FAISS_INDEX, _TROPES_LIST, _K_NEIGHBORS
        import numpy as _np

        if _FAISS_INDEX is None:
            _FAISS_INDEX = faiss.read_index(FAISS_INDEX_PATH)
            logger.info("Faiss index retrieved successfully.")


        if _TROPES_LIST is None:
            # materialize tropes from MongoDB
            tropes_cursor = get_all_tropes()
            _TROPES_LIST = list(tropes_cursor)

        X = _np.array([list(x) for x in text_embedding.tolist()]).astype(_np.float32)
        distances, indices = _FAISS_INDEX.search(X, _K_NEIGHBORS)

        results = []
        for index_row in indices:
            results.append([_TROPES_LIST[i]["name"] for i in index_row])

        return pd.Series(results)


    # @pandas_udf("double", PandasUDFType.SCALAR)
    # def llm_verification(model, tokenizer, scene_content, retrieved_tropes):
    #     """
    #     Docstring for llm_verification

    #     To apply the response generation to each line of the spark_df.

    #     :param model: The LLM model name used for response generation
    #     :param tokenizer: Param for model calling.
    #     :param scene_content: The scene content - text format.
    #     :param retrieved_tropes: The tropes retrieved in this scene (with faiss index).
    #     """
    #     # call the _generate_response function
    #     response = _generate_response(model=model, tokenizer=tokenizer, scene_content=scene_content, retrieved_tropes=retrieved_tropes)
    #     return response




    # Note : modify the defined function applied to the spark_df (incompletes)
    spark_df = spark_df.withColumn("scene_content_embeded", embed_scene_content(spark_df["scene_content"]))
    logger.info("All scene content embedded")
    spark_df = spark_df.withColumn("similar_tropes", similarity_calculation(spark_df["scene_content_embeded"]))
    logger.info("Tropes retrieved for each scene")
    #spark_df = spark_df.withColumn("LLM_output", llm_verification(model=model, tokenizer=tokenizer, scene_content=spark_df["scene_content"], retrieved_tropes=spark_df["similar_tropes"]))
    #logger.info("LLM Check ended.")

    # depending on how the last output is defined, what's next ?
    # Result formating
    # Maj of MongoDB ?

    pd_df = spark_df.toPandas()
    logger.info("Spark DataFrame converted back to pandas DataFrame.")

    # save the output locally for now (to be used for MongoDB Maj later)
    output_path = os.path.join(PRODUCTION_DATA_FOLDER, "local_analysis_output.csv")
    pd_df.to_csv(output_path, index=False)
    logger.info(f"Local analysis output saved to {output_path}")

    # ending the spark session
    spark.stop()



