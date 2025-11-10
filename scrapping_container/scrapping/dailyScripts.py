### Imports
# Diverse tools
import json
from bs4 import BeautifulSoup
import logging
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



def get_all_movie_name() :    
    DRIVER.get("https://www.dailyscript.com/movie.html")
    movie_list_elem_a_m = DRIVER.find_elements(By.XPATH, '/html/body/font/table/tbody/tr/td[1]/ul/p/a')
    movie_list_name_a_m = [elem.get_attribute("textContent") for elem in movie_list_elem_a_m]

    DRIVER.get("https://www.dailyscript.com/movie_n-z.html")
    movie_list_elem_n_z = DRIVER.find_elements(By.XPATH, '/html/body/font/table/tbody/tr/td[1]/ul/p/a')
    movie_list_name_n_z = [elem.get_attribute("textContent") for elem in movie_list_elem_n_z]

    movie_list_name = movie_list_name_a_m + movie_list_name_n_z

    movie_list_name = list(filter(lambda name : name != 'info', movie_list_name))
    movie_list_name = list(filter(lambda name : name != 'imdb', movie_list_name))

    df_daily_scripts_raw = pd.DataFrame(movie_list_name, columns=['movie_name'])
    df_daily_scripts = df_daily_scripts_raw.drop_duplicates().reset_index(drop=True)
    print(df_daily_scripts.head())
    #df_daily_scripts.to_json('daily_scripts_movie_list.json', orient='records', lines=False)
    print('------------')
    #print(movie_list_name)
    #print('------------')
    print(f"Number of movies found (DailyScripts): {len(df_daily_scripts)}")


def get_imsdb_infos() :
    base_dir = os.path.dirname(os.path.abspath(__file__))
    json_path_name_url_imsDB = os.path.join(base_dir, 'name_url_imsdb.json')
    json_path_error_imsDB = os.path.join(base_dir, 'error_imsdb.json')
    json_path_daily_scripts = os.path.join(base_dir, 'daily_scripts_movie_list.json')


    # Use pandas to load the JSON into a DataFrame (will raise if file missing)
    df_name_url = pd.read_json(json_path_name_url_imsDB)
    print(df_name_url.head())
    print(f"\nNumber of movies in the dataframe (ImsDB): {len(df_name_url)}")

    print("\n------------\n")

    # same for the error file
    df_error_name_url = pd.read_json(json_path_error_imsDB)
    print(df_error_name_url.head())
    print(f"\nNumber of movies in the error dataframe (ImsDB): {len(df_error_name_url)}")

    print("\n------------\n")

    # same for the dailyScripts file 
    df_name_daily_scripts = pd.read_json(json_path_daily_scripts)
    print(df_name_daily_scripts.head())
    print(f"\nNumber of movies on DailyScripts webpage (DailyScripts): {len(df_name_daily_scripts)}")


    return df_name_url, df_error_name_url, df_name_daily_scripts




#get_all_movie_name()   # use scrapping tools - no need it anymore because information are now stored in json file
df_name_url, df_error_name_url, df_name_daily_scripts = get_imsdb_infos()

def clean_str(chaine) :
    try :
        partie_avant, _ = chaine.rsplit(", The", 1)
        chaine_modifiee = f"The {partie_avant}"
        return chaine_modifiee
    except :
        return chaine

df_name_url['name'] = df_name_url['name'].apply(clean_str)
df_error_name_url['name'] = df_error_name_url['name'].apply(clean_str)
df_error_name_url.insert(0, 'name_error', df_error_name_url['name'])
df_error_name_url.drop(columns=['name', 'url'], inplace=True) 

df_compare = pd.merge(df_name_url, df_name_daily_scripts, left_on='name', right_on='movie_name', how='outer', indicator=True)
df_compare_error = pd.merge(df_compare, df_error_name_url, left_on='name', right_on='name_error', how='outer')
df_compare_error.drop(columns=['url'], inplace=True)
df_compare_error.to_csv('imsdb_dailyScripts_comparison_error.csv', index=False)

DRIVER.quit()