# scrapping scripts execution
#exec(open("scrapping_container/scrapping/scrapping.py").read())
# scrapping scripts execution
#exec(open("/scrapping_container/scrapping/scrapping_simplyscripts.py").read())



#import subprocess
#subprocess.run(["python", "scrapping_simplyscripts.py"], check=True)

import logging
logging.basicConfig(
    filename=f".//data_scrapping//main.log",
    filemode='w',
    level=logging.INFO
    )

logging.info("Main scrapper runner started")