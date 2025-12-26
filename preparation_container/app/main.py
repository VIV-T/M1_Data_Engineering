from ollama import Client
import json
import os
import glob
import logging
import logging.config


# new folder to create (if not existing yet)
# logs
STAGGING_FOLDER = "./project_data/stagging_data"
STAGGING_FOLDER_LOGS = f"{STAGGING_FOLDER}/logs"
STAGGING_FOLDER_SCRIPTS = f"{STAGGING_FOLDER}/scripts"
STAGGING_FOLDER_DATA = f"{STAGGING_FOLDER}/data"

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,

    "handlers": {
        "h_preparation": {
            "class": "logging.FileHandler",
            "filename": f"{STAGGING_FOLDER_LOGS}/preparation.log",
            "level": "INFO",
            "formatter": "default",
        }
    },

    "formatters": {
        "default": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        }
    },

    "loggers": {
        "preparation": {
            "handlers": ["h_preparation"],
            "level": "INFO",
            "propagate": False
        }
    }
}






def read_content(file):
    with open(file=file, mode="r", encoding='utf-8') as f :
        content = f.read()
    return content



def slice_script(full_script):
    host = os.environ.get("OLLAMA_BASE_URL", "http://ollama:11434")
    client = Client(host=host)
    prompt = f"""
    Cut the following script into separate scenes.
    For each scene, extract:
    - The scene number or title
    - The location and time (e.g., “INT. LIVING ROOM - DAY”)
    - The scene content (dialogues and actions)

    Return the result as a JSON list.

    Script :
    {full_script}
    """
    try:
        response = client.generate(model='mistral', prompt=prompt)
        if isinstance(response, dict):
            return response.get('response')
        return response
    except Exception as e:
        logging.getLogger("preparation").error(f"Failed to call Ollama at {host}: {e}")
        raise

def save_scene(scenes, dossier_sortie=os.path.join(STAGGING_FOLDER_DATA, "scenes")):
    os.makedirs(dossier_sortie, exist_ok=True)
    scenes_list = json.loads(scenes)
    for i, scene in enumerate(scenes_list):
        with open(f"{dossier_sortie}/scene_{i+1}.json", "w") as f:
            json.dump(scene, f, indent=2, ensure_ascii=False)

if __name__ == "__main__":
    # ensure directories exist before configuring logging
    os.makedirs(STAGGING_FOLDER_SCRIPTS, exist_ok=True)
    os.makedirs(STAGGING_FOLDER_LOGS, exist_ok=True)
    os.makedirs(STAGGING_FOLDER_DATA, exist_ok=True)

    logging.config.dictConfig(LOGGING)

    logger = logging.getLogger("preparation")
    logger.info("Démarrage de la préparation des données...")
    # read all the txt files in the 'project_data' volume (scripts files)
    files = glob.glob(os.path.join(STAGGING_FOLDER_SCRIPTS, "*"))
    for file in files:
        logger.info(f"Found script file: {file}")
        full_script = read_content(file)
        try:
            scenes = slice_script(full_script=full_script)
            with open(os.path.join(STAGGING_FOLDER_DATA, "debug_scenes.txt"), "a", encoding='utf-8') as debug_file:
                debug_file.write(f"\n\n# Scenes from file: {file}\n")
                debug_file.write(scenes)
            if scenes:
                save_scene(scenes)
        except Exception as e:
            logger.error(f"Skipping file {file} due to error: {e}")
    logger.info("Découpage terminé ! Les scènes sont sauvegardées dans le dossier 'scenes'.")