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


"""
Function to scrap informations from the website : https://www.dailyscript.com/
The main idea is to get the name list to compare it with the ImsDB one.
Then we decide to add url information to get the script content later.

We save those data inside a json file for later use. It avoid us to re scrapping each time.
"""
def get_all_movie_infos_dailyscripts() :    
    DRIVER.get("https://www.dailyscript.com/movie.html")
    movie_list_elem_a_m = DRIVER.find_elements(By.XPATH, '/html/body/font/table/tbody/tr/td[1]/ul/p/a')
    movie_list_name_a_m = [elem.get_attribute("textContent") for elem in movie_list_elem_a_m]
    movie_list_url_a_m = [elem.get_attribute("href") for elem in movie_list_elem_a_m]

    DRIVER.get("https://www.dailyscript.com/movie_n-z.html")
    movie_list_elem_n_z = DRIVER.find_elements(By.XPATH, '/html/body/font/table/tbody/tr/td[1]/ul/p/a')
    movie_list_name_n_z = [elem.get_attribute("textContent") for elem in movie_list_elem_n_z]
    movie_list_url_n_z = [elem.get_attribute("href") for elem in movie_list_elem_n_z]

    movie_list_name = movie_list_name_a_m + movie_list_name_n_z
    movie_list_url = movie_list_url_a_m + movie_list_url_n_z

    # dataframe creation and cleaning
    df_daily_scripts_raw = pd.DataFrame({'movie_name' : movie_list_name, 'movie_url': movie_list_url})
    df_daily_scripts_raw_filtered = df_daily_scripts_raw[df_daily_scripts_raw['movie_name'].str.lower() != 'info']
    df_daily_scripts_raw_filtered_2 = df_daily_scripts_raw_filtered[df_daily_scripts_raw_filtered['movie_name'].str.lower() != 'imdb']
    df_daily_scripts = df_daily_scripts_raw_filtered_2.drop_duplicates(subset=['movie_name']).reset_index(drop=True)
    print(df_daily_scripts.head())
    
    # file creation
    base_dir = os.path.dirname(os.path.abspath(__file__))
    json_path_daily_scripts = os.path.join(base_dir, "data_scrapping", "statistics", 'daily_scripts_movie_list.json')
    df_daily_scripts.to_json(json_path_daily_scripts, orient='records', lines=False)

    print('------------')
    print(f"Number of movies found (DailyScripts): {len(df_daily_scripts)}")


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




#get_all_movie_infos_dailyscripts()   # use scrapping tools - no need it anymore because information are now stored in json file
DRIVER.quit()       # linked to the previous line (scrapping step)

df_name_url, df_error_name_url, df_name_daily_scripts = get_existing_infos_imsdb_dailyscripts()


    

def get_url_extension_dailyscripts(url) :
    try :
        extension = url.rsplit('.', 1)[1].lower()
        return extension
    except :
        return ''
    
import requests
from io import BytesIO
def get_script_content_from_url_dailyscripts(url) :
    ext = get_url_extension_dailyscripts(url)
    if ext in [ 'html', 'htm', 'txt'] :
        try :
            response = requests.get(url)
            if response.status_code == 200 :
                content = response.content
                script_text = content.decode("utf-8", errors="ignore").replace("\n", " ")
                print(f"Successfully retrieved script from {url}")
                return script_text
            else :
                return ''
        except :
            return ''
    else :
        return ''


# to define a uniform naming convention for movie titles
import re
def to_pascal_case(string : str) -> str:
    string = string.replace(",", "")
    string = string.replace(":", "")
    string = string.replace(";", "")
    string = string.replace("'", "")
    string = string.replace(".", "")
    string = string.replace("!", "")
    string = string.replace("?", "")
    string = string.replace("III", "3")
    string = string.replace("II", "2")
    string = string.replace("IV", "4")
    words = re.split(r'[\s_-]+', string)
    
    new_string = ''.join(word.capitalize() for word in words)
    new_string = new_string.lower()
    return new_string

df_name_url['name'] = df_name_url['name'].apply(clean_str_imsdb)
df_name_url['pacal_case_name'] = df_name_url['name'].apply(to_pascal_case)


df_error_name_url['name'] = df_error_name_url['name'].apply(clean_str_imsdb)
df_error_name_url.insert(0, 'name_error', df_error_name_url['name'])
df_error_name_url.drop(columns=['name', 'url'], inplace=True) 
df_error_name_url['pacal_case_name_error'] = df_error_name_url['name_error'].apply(to_pascal_case)

df_name_daily_scripts['movie_url_extension'] = df_name_daily_scripts['movie_url'].apply(get_url_extension_dailyscripts)
df_name_daily_scripts['pacal_case_movie_name'] = df_name_daily_scripts['movie_name'].apply(to_pascal_case)
# condition over the extension of the files : avoid to have erroneous rows (link of information page instead of script)
acceptable_extensions = ['pdf', 'html', 'htm', 'txt', 'doc', 'docx', '']
df_name_daily_scripts =df_name_daily_scripts[df_name_daily_scripts['movie_url_extension'].isin(acceptable_extensions)]  



df_compare = pd.merge(df_name_url, df_name_daily_scripts, left_on='pacal_case_name', right_on='pacal_case_movie_name', how='outer', indicator=True)
df_compare_error = pd.merge(df_compare, df_error_name_url, left_on='pacal_case_name', right_on='pacal_case_name_error', how='outer')
df_compare_error.drop(columns=['url'], inplace=True)
df_compare_error.drop_duplicates(subset=['name', 'movie_name'], inplace=True)

## Code that must be used to scrapp exclusively the scripts from DailyScripts
# print("\n------------\n")
# print("Scripts scrapping started...")
# df_compare_error_scrapping = df_compare_error[df_compare_error['_merge'] != 'left_only']
# df_compare_error_scrapping['movie_script'] = df_compare_error_scrapping['movie_url'].apply(get_script_content_from_url_dailyscripts)
# print("\n------------\n")


print(df_compare_error.head())


# file creation
base_dir = os.path.dirname(os.path.abspath(__file__))
csv_path_df_compare_error = os.path.join(base_dir, 'imsdb_dailyScripts_comparison_error.csv')
df_compare_error.to_csv(csv_path_df_compare_error, index=False)



