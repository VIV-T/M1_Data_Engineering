### Imports
# Diverse tools
import json
from bs4 import BeautifulSoup
import logging
import pandas as pd

# Selenium : web navigation and scrapping
from selenium import webdriver
from selenium.webdriver.common.by import By


# Selenium driver
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--verbose')

DRIVER = webdriver.Chrome(chrome_options)
DRIVER.set_window_rect(0,0,1280,840)



def get_all_movie_name() :    
    DRIVER.get("https://www.dailyscript.com/movie.html")
    movie_list_elem_a_m = DRIVER.find_elements(By.XPATH, '/html/body/font/table/tbody/tr/td[1]/ul/p/a')
    movie_list_name_a_m = [elem.text for elem in movie_list_elem_a_m]
    
    DRIVER.get("https://www.dailyscript.com/movie_n-z.html")
    movie_list_elem_n_z = DRIVER.find_elements(By.XPATH, '/html/body/font/table/tbody/tr/td[1]/ul/p/a')
    movie_list_name_n_z = [elem.text for elem in movie_list_elem_n_z]

    movie_list_name = movie_list_name_a_m + movie_list_name_n_z

    movie_list_name = list(filter(lambda name : name != 'imdb', movie_list_name))
    print(movie_list_name)


get_all_movie_name()

DRIVER.quit()