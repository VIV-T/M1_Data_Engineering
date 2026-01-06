### --Imports
import logging
import logging.config
import os

from local_analysis import main_local_analysis

### --Initialization--
# logs
PRODUCTION_DATA_FOLDER = "./project_data/production_data"
PRODUCTION_LOGS_FOLDER = os.path.join(PRODUCTION_DATA_FOLDER, "logs")

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,

    "handlers": {
        "h_local_analysis": {
            "class": "logging.FileHandler",
            "filename": f"{PRODUCTION_LOGS_FOLDER}/local_analysis.log",
            "level": "INFO",
            "formatter": "default",
        },
        "h_global_analysis": {
            "class": "logging.FileHandler",
            "filename": f"{PRODUCTION_LOGS_FOLDER}/global_analysis.log",
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
        "local_analysis": {
            "handlers": ["h_local_analysis"],
            "level": "INFO",
            "propagate": False
        },
        "global_analysis": {
            "handlers": ["h_global_analysis"],
            "level": "INFO",
            "propagate": False
        },
    }
}


def main() :
    logging.config.dictConfig(LOGGING)
    main_local_analysis()

main()