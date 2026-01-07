### LLM related - hardware limitations

# ## Imports
# import logging 
# import os
# import json
# import torch
# import pandas as pd  # Correction : import standard
# import numpy as np
# from pymongo import MongoClient
# from bson.objectid import ObjectId
# from sentence_transformers import SentenceTransformer
# import faiss

# # To use a LLM from HuggingFace
# from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline

# # Logger config 
# logger = logging.getLogger("global_analysis")
# logging.basicConfig(level=logging.INFO) # Assure que les logs s'affichent

# # Env variables
# MODEL_PATH = os.getenv("MODEL_PATH", "path/to/model")
# FAISS_INDEX_PATH = os.getenv("FAISS_INDEX_PATH", "path/to/index")
# LLM_NAME = "mistralai/Mistral-7B-Instruct-v0.2"

# ## MongoDB tools

# def _connect_mongoDB():
#     client = MongoClient(
#         host="mongodb://mongo:27017/",
#         username="admin",
#         password="admin"
#     )
#     db = client["scriptsDB"]
#     return db

# def _check_connection_mongoDB():
#     try:
#         _connect_mongoDB()
#         logger.info("Success connecting to MongoDB")
#     except Exception as e:
#         logger.error(f"Error connecting to MongoDB: {e}")
#         exit(1)

# def get_all_movies():
#     db = _connect_mongoDB()
#     movies_collection = db["movies"]
    
#     movies = list(movies_collection.find({}))
#     logger.info(f"Retrieved {len(movies)} movies from MongoDB.")
#     return movies

# def get_all_tropes():
#     db = _connect_mongoDB()
#     tropes_collection = db["tropes"]
#     tropes = list(tropes_collection.find({}))
#     logger.info(f"Retrieved {len(tropes)} tropes from MongoDB.")
#     return tropes

# def format_data(movies: list):
#     dict_formated = {"_id": [], "movie_name": [], "scene_content": [], "full_script": []}
#     logger.info("Starting formatting data with full scripts")

#     for movie in movies:
#         script_text = movie.get("full_script", "")
#         movie_id = str(movie["_id"])
#         movie_name = movie.get("name", "Unknown")
        
#         for scene in movie.get("scenes", []):
#             dict_formated["_id"].append(movie_id)
#             dict_formated["movie_name"].append(movie_name)
#             dict_formated["scene_content"].append(scene)
#             dict_formated["full_script"].append(script_text)

#     df_formated = pd.DataFrame(dict_formated)
#     logger.info(f"Formatting complete: {len(df_formated)} rows created.")
#     return df_formated

# def _generate_response(model, tokenizer, full_script, retrieved_tropes):
#     """Generate LLM response for global analysis."""
    
#     # security if no tropes retrieved
#     tropes_str = ", ".join(retrieved_tropes) if retrieved_tropes else "None"
    
#     task = "Analyze this **movie script** and confirm the presence of the following **tropes**."
#     prompt = f"""
#     ASSUME YOU ARE AN EXPERT IN MOVIE ANALYSIS AND TROPES IDENTIFICATION.
#     {task}

#     **movie script**: {full_script}

#     Potential **tropes** : {tropes_str}

#     ### INSTRUCTIONS:
#     1. Confirm if the tropes are actually present in this movie script.
#     2. If a trope is confirmed, provide a short example from the text.
#     3. Return a JSON object if tropes are found, otherwise return an empty string "".
    
#     ### EXPECTED OUTPUT:
#     JSON: {{"tropes": ["name"], "examples": ["text"]}} OR ""
#     """

#     inputs = tokenizer(prompt, return_tensors="pt").to(model.device)
    
#     with torch.no_grad():
#         output_tokens = model.generate(
#             **inputs, 
#             max_new_tokens=512, 
#             temperature=0.1,
#             do_sample=False 
#         )
    
#     full_response = tokenizer.decode(output_tokens[0], skip_special_tokens=True)
    
#     response_text = full_response[len(prompt):].strip()
#     return response_text

# def _save_analysis_to_mongodb(df_results):
#     """
#     Save the 'global_analysis' field in the movies collection.
#     """
#     db = _connect_mongoDB()
#     movies_collection = db["movies"]

#     logger.info(f"Starting MongoDB update for {len(df_results)} records.")

#     for _, row in df_results.iterrows():
#         try:
#             movie_id = ObjectId(row["_id"])
#             analysis_result = row["llm_check"]

#             movies_collection.update_one(
#                 {"_id": movie_id},
#                 {"$set": {"global_analysis": analysis_result}}
#             )
            
#             logger.info(f"Updated global_analysis for: {row['movie_name']}")
            
#         except Exception as e:
#             logger.error(f"Failed to update movie {row.get('movie_name', 'Unknown')}: {e}")

#     logger.info("MongoDB update process completed.")

# def main_global_analysis():

#     logger.info("Global analysis started !")

#     # embedding model and faiss index
#     MODEL_EMBEDDING = SentenceTransformer(MODEL_PATH)
#     logger.info("Embedding model retrieved successfully.")
#     _FAISS_INDEX = faiss.read_index(FAISS_INDEX_PATH)
#     logger.info("Faiss index retrieved successfully.")

#     # Load sentence transformer model
#     _TOKENIZER = AutoTokenizer.from_pretrained(LLM_NAME)
#     logger.info("LLM tokenizer initialized.")
#     _MODEL = AutoModelForCausalLM.from_pretrained(LLM_NAME, torch_dtype=torch.float16, device_map="auto")
#     logger.info("LLM model initialized.")

#     # Check MongoDB connection
#     _check_connection_mongoDB()

#     # Retrieve data from MongoDB
#     movies = get_all_movies()
#     tropes = get_all_tropes()
#     _TROPES_LIST = list(tropes)

#     # Format data
#     df_formated = format_data(movies)

#     # Global analysis loop for each movie
#     results_list = []

#     for movie in movies:
#         movie_name = movie.get("name", "Unknown")
#         movie_id = str(movie["_id"])
#         full_script = movie.get("full_script", "")
        
#         if not full_script:
#             logger.warning(f"Skipping {movie_name}: No full_script found.")
#             continue

#         logger.info(f"Analyzing movie: {movie_name}")

#         # Retrieve tropes with FAISS
#         script_embedding = MODEL_EMBEDDING.encode([full_script], convert_to_numpy=True)
#         _, indices = _FAISS_INDEX.search(script_embedding.astype(np.float32), 5)
        
#         retrieved_tropes = [_TROPES_LIST[i]["name"] for i in indices[0]]
        
#         # Check retrieved tropes with LLM
#         try:
#             analysis_result = _generate_response(
#                 model=_MODEL, 
#                 tokenizer=_TOKENIZER, 
#                 full_script=full_script, 
#                 retrieved_tropes=retrieved_tropes
#             )
            
#             results_list.append({
#                 "_id": movie_id,
#                 "movie_name": movie_name,
#                 "llm_check": analysis_result
#             })
#         except Exception as e:
#             logger.error(f"Error during LLM inference for {movie_name}: {e}")

#     # Conversion results in DataFrame and save to MongoDB
#     if results_list:
#         df_results = pd.DataFrame(results_list)
#         _save_analysis_to_mongodb(df_results)
#         logger.info("Global analysis completed successfully!")
#     else:
#         logger.warning("No results to save.")
    
#     logger.info("Global analysis completed !")



