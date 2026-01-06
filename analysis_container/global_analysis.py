## Imports
import logging 
import os
from sentence_transformers import SentenceTransformer
import faiss


# logger config 
logger = logging.getLogger("global_analysis")

# env variables
MODEL_PATH = os.environ["MODEL_PATH"]
FAISS_INDEX_PATH = os.environ["FAISS_INDEX_PATH"]

LLM_NAME = "mistralai/Mistral-7B-Instruct-v0.2"


def main_global_analysis():
    logger.info("Global analysis started !")


