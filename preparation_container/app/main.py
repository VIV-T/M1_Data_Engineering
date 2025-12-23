from ollama import Client
import json
import os
import glob
import logging
import logging.config

# volume related
VOLUME_FOLDER = os.path.join("/opt", "airflow", "project_data")
# new folder to create (if not existing yet)
STAGGING_DATA_FOLDER = os.path.join(VOLUME_FOLDER, "stagging_data")
STAGGING_FOLDER_SCRIPTS = os.path.join(STAGGING_DATA_FOLDER, "scripts")
STAGGING_FOLDER_LOGS = os.path.join(STAGGING_DATA_FOLDER, "logs")



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
    client = Client(host='http://localhost:11434')
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
    response = client.generate(model='mistral', prompt=prompt)
    return response['response']

def save_scene(scenes, dossier_sortie="scenes"):
    os.makedirs(dossier_sortie, exist_ok=True)
    scenes_list = json.loads(scenes)
    for i, scene in enumerate(scenes_list):
        with open(f"{dossier_sortie}/scene_{i+1}.json", "w") as f:
            json.dump(scene, f, indent=2, ensure_ascii=False)

if __name__ == "__main__":
    logging.config.dictConfig(LOGGING)
    logging.info("Démarrage de la préparation des données...")
    # read all the txt files in the 'project_data' volume (scripts files)
    files = glob.glob(os.path.join(STAGGING_FOLDER_SCRIPTS, "*"))
    for file in files:
        logging.info(f"Found script file: {file}")
        full_script = read_content(files[0])
        scenes = slice_script(full_script=full_script)
        save_scene(scenes)
    logging.info("Découpage terminé ! Les scènes sont sauvegardées dans le dossier 'scenes'.")