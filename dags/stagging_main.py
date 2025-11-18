### --Imports
import logging
import logging.config

import stagging_pdf_content_extraction_ocr


### --Initialization--
# logs
STAGGING_FOLDER = ".//volume//stagging_data"
STAGGING_FOLDER_LOGS = f"{STAGGING_FOLDER}//logs"

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,

    "handlers": {
        "h_stagging_pdf_content_extraction_ocr": {
            "class": "logging.FileHandler",
            "filename": f"{STAGGING_FOLDER_LOGS}//pdf_content_extraction_ocr.log",
            "level": "INFO",
            "formatter": "default",
        },
        "h_filename_html_cleaning": {
            "class": "logging.FileHandler",
            "filename": f"{STAGGING_FOLDER_LOGS}//filename_html_cleaning.log",
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
        "filename_html_cleaning": {
            "handlers": ["h_filename_html_cleaning"],
            "level": "INFO",
            "propagate": False
        },
    }
}


def main() :
    logging.config.dictConfig(LOGGING)
    stagging_pdf_content_extraction_ocr.main_stagging_ocr()
    #scrapping_imsdb.main_scrapping_imsDB()

main()