import logging


SCRAPPING_FOLDER = ".//data_scrapping"
SCRAPPING_FOLDER_DATA = f"{SCRAPPING_FOLDER}//data"   # adapt your outputs later in your code based on those variables.
SCRAPPING_FOLDER_LOGS = f"{SCRAPPING_FOLDER}//logs"


logger = logging.getLogger("test_1")


def main_test_1():
    logger.info("Test log 1 initialized")