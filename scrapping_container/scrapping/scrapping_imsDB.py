### --Imports--
# Diverse tools
from copy import deepcopy
import logging
import pandas as pd

# Selenium : web navigation and scrapping
from selenium import webdriver
from selenium.webdriver.common.by import By




### --Initialization--
# volumes & log folder - initialization
SCRAPPING_FOLDER = ".//data_scrapping"
SCRAPPING_FOLDER_DATA = f"{SCRAPPING_FOLDER}//data" # adapt your outputs later in your code based on those variables.
SCRAPPING_FOLDER_LOGS = f"{SCRAPPING_FOLDER}//logs"


logger = logging.getLogger("scrapping_imsdb")


# Selenium driver - initialization
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--verbose')



###--Tools--
# Create the alphabetical index (based on the website structure)
def _initialize_alpha_index():
    alphabetical_index = []
    # index 0
    alphabetical_index.append(str(0))
    # All the MAJ letter
    for i in range(65, 91):
        alphabetical_index.append(chr(i))

    logger.info("[I] Alpha index initialized")
    return alphabetical_index


def build_url_from_name(name : str) :
    modified_name = name.replace(": ", "-")
    modified_name = modified_name.replace("%", "%2526")
    url = modified_name.replace(" ", "-")
    logger.info(f"[I] build_url_from_name - Builded url : {url}")
    return url


# get the name_list and url_list to next scrapp the ressources
def _get_name_url_list(alphabetical_index : str) : 
    name_list = []
    for alpha_index in alphabetical_index :
        # go to the website url
        DRIVER.get(f'https://imsdb.com/alphabetical/{alpha_index}')
        
        # find the namelist base on the alphabetical index
        web_elem_list = DRIVER.find_elements(by="xpath", value="//*[@id='mainbody']/table[2]/tbody/tr/td[3]//a")
        name_list.extend(list(map(lambda elem : elem.text, web_elem_list)))
        logger.info(f"[I] _get_name_url_list - Found {len(web_elem_list)} movie names for index {alpha_index}")

    # build 'url_list' based on 'name_list' (cf. url structure on the website - html ressources) 
    url_list = deepcopy(name_list)
    url_list = list(map(lambda elem : build_url_from_name(elem), url_list))
    
    return name_list, url_list




### --Main function--
# To scrap the imsdb website
def main_scrapping_imsDB() :    
    
    logger.info("[I] Scrapper started")

    global DRIVER 
    DRIVER = webdriver.Chrome(chrome_options)
    DRIVER.set_window_rect(0,0,1280,840)

    # initialization
    alpha_index = _initialize_alpha_index()

    movie_name_list, url_list = _get_name_url_list(alpha_index)
    
    df_imsdb = pd.DataFrame({
        'movie_name_imsdb' : movie_name_list,
        'html_url' : url_list
    })

    # Save the data scrapped into a csv file (store in the docker volume)
    df_imsdb.to_csv(f"{SCRAPPING_FOLDER_DATA}//imsdb_data.csv", index=False)

    logger.info(f"[I] {len(df_imsdb)} movie url scrapped")
    logger.info("[I] Scrapper finished")


