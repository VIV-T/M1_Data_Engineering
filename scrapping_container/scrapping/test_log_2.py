import logging


SCRAPPING_FOLDER = ".//data_scrapping"
SCRAPPING_FOLDER_DATA = f"{SCRAPPING_FOLDER}//data"   # adapt your outputs later in your code based on those variables.
SCRAPPING_FOLDER_LOGS = f"{SCRAPPING_FOLDER}//logs"




logger = logging.getLogger("test_2")


def main_test_2():
    logger.info("Test log 2 initialized")
