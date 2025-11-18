# first step : based on the main link 
#       |_ get all the url to each movie script page after loading all the movie available in the main page

# second step : based on each movie script page link
#       |_ get the pdf url to download the script later.
# 
# Nt : the script download and the content extraction will be done in another python script.        

### --Imports--
import pandas as pd
import time
import logging
import random as rd

# Selenium : web navigation and scrapping
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC



### --Initialization--
# volumes & log folder - initialization
SCRAPPING_FOLDER = ".//volume//scrapping_data"
SCRAPPING_FOLDER_DATA = f"{SCRAPPING_FOLDER}//data"   # adapt your outputs later in your code based on those variables.
SCRAPPING_FOLDER_LOGS = f"{SCRAPPING_FOLDER}//logs"

# logger (based on the "main.py" script config)
logger = logging.getLogger("scrapping_script_slug")


# Selenium driver - initialization
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--verbose')

# Global variables
MIN_DELAY_S = 0.6
MAX_DELAY_S = 1.5


### --Tools--
# To make the program sleep for random time bounded by MIN_DELAY_S and MAX_DELAY_S 
#       => avoid to be block by the website during the scrapping
def _define_time_sleep(): 
    return rd.uniform(MIN_DELAY_S, MAX_DELAY_S)


# handle the consent popup if present - avoid to lock the web navigtation
def handle_consent_popup():    
    try:
        # Wait for the consent button to be clickable
        consent_button = WebDriverWait(DRIVER, _define_time_sleep()).until(
            EC.element_to_be_clickable((By.XPATH, "/html/body/div[5]/div[2]/div[2]/div[3]/div[2]/button[1]"))
        )
        # Click the consent button
        consent_button.click()
        logger.info(f"handle_consent_pop - consent popup handled") 
    except Exception as e:
        logger.error(f"handle_consent_pop - Could not find or click the consent button:", e) 



# Due to the website structure (dynamically generated content), we need to click on the "Load More" button until all the scripts are loaded
def load_all_data() :  
    error_count = 0
    while True :
        try :
            load_more_script_button = DRIVER.find_element(By.XPATH, '//*[@id="scriptsList"]/div[6]/button')  
            DRIVER.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'center'});", load_more_script_button)
            load_more_script_button.click()
            time.sleep(_define_time_sleep())
        except Exception as e :
            # check if we have all the data load - if the last movie load first's letter != 'z' => all the movies aren't loaded
            movie_name_list_web_element = DRIVER.find_elements(By.XPATH, '//*[@id="scriptsList"]/div[3]/a/div/p[1]')
            last_elem_name = movie_name_list_web_element[-1].get_attribute('innerText')
            if last_elem_name[0].lower() != 'z' :
                time.sleep(_define_time_sleep())
                continue
            else :
                # to handle the case where there is no more "Load More" button to click + let the driver the time to load the page content
                error_count += 1
                if error_count < 3 :
                    time.sleep(_define_time_sleep())
                    continue
                else :
                    logger.info(f"load_all_data - No more 'Load More' button to click :", e)
                    break




# based on the main page, we scrap all the movie page urls available (where it is possible to download the script pdf)
def get_all_movie_script_page_urls() :
    movie_list_container = DRIVER.find_element(By.XPATH, '//*[@id="scriptsList"]/div[3]')
    movie_list_web_elem = movie_list_container.find_elements(By.TAG_NAME, 'a')

    movie_name_list_web_element = DRIVER.find_elements(By.XPATH, '//*[@id="scriptsList"]/div[3]/a/div/p[1]')
    movie_name_list = [movie_name.get_attribute('innerText') for movie_name in movie_name_list_web_element]

    url_list = []
    for movie in movie_list_web_elem : 
        url = movie.get_attribute('href')
        url_list.append(url)
        logger.info(f"get_all_movie_script_page_urls - Found movie script page url : {url}")
    
    logger.info(f"get_all_movie_script_page_urls - Total movie script page urls found : {len(url_list)}")
    return url_list, movie_name_list



# based on the previous html movie script page, we get the pdf script url to download the script.
# # /!\ the output here isn't the pdf, but the link to download the pdf ! 
def get_pdf_script_url(url) :
    time.sleep(_define_time_sleep())    # add a random time sleep to avoid the website block
    DRIVER.get(url)
    try : 
        pdf_script_web_elem = DRIVER.find_element(By.XPATH, '/html/body/main/div/div/article/div/div[2]/div[1]/div[1]/a')
        pdf_script_url = pdf_script_web_elem.get_attribute('href')
        logger.info(f"get_pdf_script_url - Found pdf script url : {pdf_script_url}")
        return pdf_script_url  
    except Exception as e :
        logger.error(f"get_pdf_script_url - Error while scrapping this url {pdf_script_url} - error :", e)
        return None
    

### --Main function--
# To scrap the script slug website
def main_scrapping_script_slug() :
    logger.info(f"Scrapper started")

    global DRIVER 
    DRIVER = webdriver.Chrome(chrome_options)
    DRIVER.set_window_rect(0,0,1280,840)

    DRIVER.get('https://www.scriptslug.com/scripts/medium/film?sort=az')
    handle_consent_popup()

    load_all_data()      # + of 20 min. to load all the data

    url_list, movie_name_list = get_all_movie_script_page_urls()

    # /!\ the column are following a precise naming convention : "{field_name}_{source_name}"
    #   |_ field_name = "url"
    #   |_ source_name = "script_slug"
    # It will be useful in Airflow DAG later
    df_script_slug = pd.DataFrame({
        'movie_name_script_slug' : movie_name_list,
        'script_page_url' : url_list
    })

    df_script_slug["url_script_slug"] = df_script_slug["script_page_url"].apply(get_pdf_script_url)   # take also a long time to execute
    logger.info(f"Pdf script urls scrapped and added to the dataframe")
    df_script_slug = df_script_slug.dropna(subset=['url_script_slug']) # drop the lines where we didn't find any pdf url (only 1 line dropped here : 'Fighting with My Family' movie)
    df_script_slug_clean  = df_script_slug.reset_index(drop=True)
    df_script_slug_clean = df_script_slug_clean.drop("script_page_url", axis=1)   # drop the script page url column (not useful later)

    # Save the data scrapped into a csv file (store in the docker volume)
    df_script_slug_clean.to_csv(f"{SCRAPPING_FOLDER_DATA}//script_slug_data.csv", index=False, encoding='utf-8')

    DRIVER.quit()

    logger.info(f"Scrapper finished")