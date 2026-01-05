### --Imports
import logging
import logging.config


### --Initialization--
# logs
PRODUCTION_FOLDER = ""
PRODUCTION_FOLDER_LOGS = ""

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,

    "handlers": {
        "h_local_analysis": {
            "class": "logging.FileHandler",
            "filename": f"{PRODUCTION_FOLDER_LOGS}/local_analysis.log",
            "level": "INFO",
            "formatter": "default",
        },
        "h_global_analysis": {
            "class": "logging.FileHandler",
            "filename": f"{PRODUCTION_FOLDER_LOGS}/global_analysis.log",
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

main()