### --Imports
import logging
import logging.config

import stagging_pdf_content_extraction_ocr
import stagging_html_cleaning


### --Initialization--
# logs
STAGGING_FOLDER = "./project_data/stagging_data"
STAGGING_FOLDER_LOGS = f"{STAGGING_FOLDER}/logs"

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,

    "handlers": {
        "h_stagging_pdf_content_extraction_ocr": {
            "class": "logging.FileHandler",
            "filename": f"{STAGGING_FOLDER_LOGS}/pdf_content_extraction_ocr.log",
            "level": "INFO",
            "formatter": "default",
        },
        "h_stagging_html_cleaning": {
            "class": "logging.FileHandler",
            "filename": f"{STAGGING_FOLDER_LOGS}/stagging_html_cleaning.log",
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
        "stagging_pdf_content_extraction_ocr": {
            "handlers": ["h_stagging_pdf_content_extraction_ocr"],
            "level": "INFO",
            "propagate": False
        },
        "stagging_html_cleaning": {
            "handlers": ["h_stagging_html_cleaning"],
            "level": "INFO",
            "propagate": False
        },
    }
}


def main() :
    logging.config.dictConfig(LOGGING)
    stagging_html_cleaning.main_stagging_html_cleaning()
    stagging_pdf_content_extraction_ocr.main_stagging_ocr()

main()