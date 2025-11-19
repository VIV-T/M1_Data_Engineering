### --Imports
import logging
import logging.config
import shutil

import scrapping_script_slug
import scrapping_imsdb


### --Initialization--

# volume content : 
# copy the folder '' from 'app/scrapping_container/scrapping_files/scrapping_data' to 'app/project_data/scrapping_data'
shutil.copytree("./scrapping_container/scrapping_files/scrapping_data", "./project_data/scrapping_data", dirs_exist_ok=True)   

# logs
SCRAPPING_FOLDER = "./project_data/scrapping_data"
SCRAPPING_FOLDER_LOGS = f"{SCRAPPING_FOLDER}/logs"

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,

    "handlers": {
        "h_scrapping_script_slug": {
            "class": "logging.FileHandler",
            "filename": f"{SCRAPPING_FOLDER_LOGS}//scrapping_script_slug.log",
            "level": "INFO",
            "formatter": "default",
        },
        "h_scrapping_imsdb": {
            "class": "logging.FileHandler",
            "filename": f"{SCRAPPING_FOLDER_LOGS}//scrapping_imsdb.log",
            "level": "INFO",
            "formatter": "default",
        },
    },

    "formatters": {
        "default": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        }
    },

    "loggers": {
        "scrapping_script_slug": {
            "handlers": ["h_scrapping_script_slug"],
            "level": "INFO",
            "propagate": False
        },
        "scrapping_imsdb": {
            "handlers": ["h_scrapping_imsdb"],
            "level": "INFO",
            "propagate": False
        },
    }
}


def main() :
    # config the logs based on the previous precised configuration
    logging.config.dictConfig(LOGGING)

    # call the main scrapping function for the 2 data sources
    scrapping_script_slug.main_scrapping_script_slug()
    scrapping_imsdb.main_scrapping_imsDB()

main()