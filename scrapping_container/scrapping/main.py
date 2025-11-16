### --Imports
import logging
import logging.config
import sys

import scrapping_scripts_slug
import scrapping_imsdb


### --Initialization--
# logs
SCRAPPING_FOLDER = ".//data_scrapping"
SCRAPPING_FOLDER_LOGS = f"{SCRAPPING_FOLDER}//logs"

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,

    "handlers": {
        "h_scrapping_scripts_slug": {
            "class": "logging.FileHandler",
            "filename": f"{SCRAPPING_FOLDER_LOGS}//scrapping_scripts_slug.log",
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
        "scrapping_scripts_slug": {
            "handlers": ["h_scrapping_scripts_slug"],
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
    logging.config.dictConfig(LOGGING)
    scrapping_scripts_slug.main_scrapping_scripts_slug()
    scrapping_imsdb.main_scrapping_imsDB()

main()