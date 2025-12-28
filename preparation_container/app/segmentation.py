### Imports
import re
import os
import logging
import logging.config
import glob

# Volume related folders 
STAGGING_FOLDER = "./project_data/stagging_data"
STAGGING_FOLDER_LOGS = f"{STAGGING_FOLDER}/logs"
STAGGING_FOLDER_SCRIPTS = f"{STAGGING_FOLDER}/scripts"
STAGGING_FOLDER_DATA = f"{STAGGING_FOLDER}/data"

# logs 

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,

    "handlers": {
        "h_segmentation": {
            "class": "logging.FileHandler",
            "filename": f"{STAGGING_FOLDER_LOGS}/segmentation.log",
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
        "segmentation": {
            "handlers": ["h_segmentation"],
            "level": "INFO",
            "propagate": False
        }
    }
}



def decouper_script_par_scenes(file_path):
    """
    Découpe un script de film en scènes en utilisant des marqueurs courants.

    Args:
        chemin_fichier (str): Chemin vers le fichier contenant le script.

    Returns:
        list: Liste des scènes extraites.
    """
    with open(file_path, 'r', encoding='utf-8') as file:
        script = file.read()

    # Expression régulière pour identifier les en-têtes de scène
    # Exemples de motifs : "INT. SALON - JOUR", "EXT. PARC - NUIT", etc.
    motif_scene = re.compile(
        r"(\bINT\.\s.*?$|\bEXT\.\s.*?$|\bFADE IN:.*?$|\bFADE OUT\..*?$|\bCUT TO:.*?$|\bSCÈNE.*?$)",
        re.IGNORECASE | re.MULTILINE
    )

    # Découper le script en utilisant les marqueurs de scène
    positions_scenes = [match.start() for match in motif_scene.finditer(script)]
    positions_scenes.append(len(script))  # Ajouter la fin du script

    scenes = []
    for i in range(len(positions_scenes) - 1):
        debut = positions_scenes[i]
        fin = positions_scenes[i + 1]
        scene = script[debut:fin].strip()
        scenes.append(scene)

    return scenes

def sauvegarder_scenes(movie_name, scenes, dossier_sortie):
    """
    Sauvegarde chaque scène dans un fichier séparé.

    Args:
        scenes (list): Liste des scènes à sauvegarder.
        chemin_sortie (str): Chemin du dossier où sauvegarder les scènes.
    """
    import os
    if not os.path.exists(dossier_sortie):
        os.makedirs(dossier_sortie)

    for i, scene in enumerate(scenes, start=1):
        nom_fichier = os.path.join(dossier_sortie, movie_name+f"_scene_{i}.txt")
        with open(nom_fichier, 'w', encoding='utf-8') as fichier:
            fichier.write(scene)



if __name__ == "__main__":

    # ensure directories exist before configuring logging
    os.makedirs(STAGGING_FOLDER_SCRIPTS, exist_ok=True)
    os.makedirs(STAGGING_FOLDER_LOGS, exist_ok=True)
    os.makedirs(STAGGING_FOLDER_DATA, exist_ok=True)

    logging.config.dictConfig(LOGGING)

    logger = logging.getLogger("segmentation")
    logger.info("Démarrage de la segmentation des données...")
        


    # Exemple d'utilisation
    files = glob.glob(os.path.join(STAGGING_FOLDER_SCRIPTS, "*"))
    for file_path in files:
        movie_name = os.path.basename(file_path).split('.')[0]
        logger.info(f"Found script file: {file_path}")
        scenes = decouper_script_par_scenes(file_path=file_path)

        # Afficher le nombre de scènes détectées
        logger.info(f"Nombre de scènes détectées : {len(scenes)}")

        # Sauvegarder les scènes dans un dossier
        dossier_sortie = os.path.join(STAGGING_FOLDER_DATA, "scenes")
        sauvegarder_scenes(movie_name, scenes, dossier_sortie)
        logger.info(f"Les scènes ont été sauvegardées dans le dossier : {dossier_sortie}")
