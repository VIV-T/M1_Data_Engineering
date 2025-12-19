### --Imports
import logging
import logging.config
import shutil
import os
import stat

import scrapping_script_slug
import scrapping_imsdb


### --Initialization--

# volume content : 
# copy the folder '' from 'app/scrapping_container/scrapping_files/scrapping_data' to 'app/project_data/scrapping_data'
shutil.copytree("./scrapping_container/scrapping_files/scrapping_data", "./project_data/scrapping_data", dirs_exist_ok=True)   

# Ensure permissive permissions and correct ownership so Airflow can read the files from the shared volume.
def _set_permissions_and_ownership(path, airflow_uid_env="AIRFLOW_UID"):
    try:
        uid_str = os.environ.get(airflow_uid_env)
        uid = int(uid_str) if uid_str is not None else None
    except Exception:
        uid = None

    for root, dirs, files in os.walk(path):
        for d in dirs:
            p = os.path.join(root, d)
            try:
                os.chmod(p, 0o775)
                if uid is not None:
                    os.chown(p, uid, 0)
            except Exception as e:
                logging.warning("Cannot set perms/ownership on dir %s: %s", p, e)
        for f in files:
            p = os.path.join(root, f)
            try:
                os.chmod(p, 0o664)
                if uid is not None:
                    os.chown(p, uid, 0)
            except Exception as e:
                logging.warning("Cannot set perms/ownership on file %s: %s", p, e)

try:
    _set_permissions_and_ownership("./project_data/scrapping_data")
except Exception:
    logging.exception("Failed to set permissions/ownership on project_data/scrapping_data")

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