import logging
import logging.config
import os 
from analysis_container.hybrid_analysis import main_analysis

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
        "h_analysis": {
            "class": "logging.FileHandler",
            "filename": f"{STAGGING_FOLDER_LOGS}/analysis.log",
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
        "analysis": {
            "handlers": ["h_analysis"],
            "level": "INFO",
            "propagate": False
        }
    }
}



if __name__ == "__main__":
    # ensure directories exist before configuring logging
    os.makedirs(STAGGING_FOLDER_SCRIPTS, exist_ok=True)
    os.makedirs(STAGGING_FOLDER_LOGS, exist_ok=True)
    os.makedirs(STAGGING_FOLDER_DATA, exist_ok=True)

    logging.config.dictConfig(LOGGING)

    logger = logging.getLogger("analysis")

    logger.info("Analysis started.")
    
    main_analysis()

    logger.info("Analysis completed.")