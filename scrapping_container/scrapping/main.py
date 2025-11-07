# scrapping scripts execution

import logging
logging.basicConfig(
    filename=f".//data_scrapping//scrapping.log",
    filemode='w',
    level=logging.INFO
    )

logging.info("Main scrapper runner started")

import scrapping_simplyscripts
import scrapping_imsDB



#scrapping_simplyscripts.main_scrapping_simplyScripts()

scrapping_imsDB.main_scrapping_imsDB()