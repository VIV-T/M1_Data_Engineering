# first step : based on the main link 
#       |_ get all the url to each movie script page after loading all the movie available in the main page

# second step : based on each movie script page link
#       |_ get the pdf url to download the script before passing it to the ocr. 
#       |_ download the script pdf file in the docker volume dedicated to scrapping scripts        

import pandas as pd
# Selenium : web navigation and scrapping
from selenium import webdriver
from selenium.webdriver.common.by import By
import os

# Selenium driver
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--verbose')

DRIVER = webdriver.Chrome(chrome_options)
DRIVER.set_window_rect(0,0,1280,840)


def get_all_movie_script_page_urls(main_url) :
    DRIVER.get(main_url)
    load_more_script_button = DRIVER.find_element(By.XPATH, '//*[@id="scriptsList"]/div[6]/button')


# end of the scrapping tasks
DRIVER.quit()