### Imports
import re
import os
import logging
import glob

from shared_operators import _check_connection_mongoDB, _connect_mongoDB

# Volume-related folders
STAGGING_FOLDER = "./project_data/stagging_data"
STAGGING_FOLDER_LOGS = f"{STAGGING_FOLDER}/logs"
STAGGING_FOLDER_SCRIPTS = f"{STAGGING_FOLDER}/scripts"
STAGGING_FOLDER_DATA = f"{STAGGING_FOLDER}/data"


def slice_script(logger, file_path):
    """
    Split a script into scenes using common screenplay markers.

    Args:
        logger (logging.Logger): Logger instance.
        file_path (str): Path to the script file.

    Returns:
        list: List of extracted scenes.
    """
    with open(file_path, 'r', encoding='utf-8') as file:
        script = file.read()

    # Regular expression to identify scene headers
    # Examples: "INT. LIVING ROOM - DAY", "EXT. PARK - NIGHT", etc.
    scene_pattern = re.compile(
        r"(\bINT\.\s.*?$|\bEXT\.\s.*?$|\bFADE IN:.*?$|\bFADE OUT\..*?$|\bCUT TO:.*?$|\bSCÈNE.*?$)",
        re.IGNORECASE | re.MULTILINE
    )

    # Find the position of the first scene
    first_scene = scene_pattern.search(script)

    # Extract the introduction (text before the first scene)
    introduction = script[:first_scene.start()].strip() if first_scene else script.strip()

    # Split the script using scene markers
    scene_positions = [match.start() for match in scene_pattern.finditer(script)]
    scene_positions.append(len(script))  # Add end of script

    scenes = []
    for i in range(len(scene_positions) - 1):
        start = scene_positions[i]
        end = scene_positions[i + 1]
        scene = script[start:end].strip()
        scenes.append(scene)

    # Insert the introduction as the first element
    scenes.insert(0, introduction)
    logger.info(scenes)
    return scenes


def save_scenes(logger, movie_name, scenes):
    """
    Save each scene into MongoDB.

    Args:
        logger (logging.Logger): Logger instance.
        movie_name (str): Movie name.
        scenes (list): List of scenes to save.
    """
    # Check if MongoDB connection is available
    _check_connection_mongoDB()

    script_DB = _connect_mongoDB()
    movie_collection = script_DB["movies"]

    movie_collection.update_one(
        {"name": movie_name},
        {"$set": {
            "scenes": scenes
        }}
    )

    logger.info(f"Saved {len(scenes)} scenes for movie: {movie_name} in MongoDB")


# Main function to segment and save scenes from a single script file
def main_segmentation_script(logger, file_path):
    movie_name = os.path.basename(file_path).split('.')[0]
    logger.info(f"Found script file: {file_path}")
    scenes = slice_script(logger, file_path=file_path)

    # Save scenes to MongoDB
    save_scenes(logger, movie_name, scenes)


# Function to segment and save scenes from all scripts
# Intended for use in a DAG (batch processing)
def main_segmentation():
    files = glob.glob(os.path.join(STAGGING_FOLDER_SCRIPTS, "*"))

    logger = logging.getLogger(__name__)
    logger.info("Starting script segmentation process...")

    for file_path in files:
        logger.info(f"Processing file: {file_path}")
        main_segmentation_script(logger, file_path)

    logger.info("Script segmentation process completed.")
    return True