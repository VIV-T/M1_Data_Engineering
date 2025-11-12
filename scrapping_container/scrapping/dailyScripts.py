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
    json_path_daily_scripts = os.path.join(base_dir, 'daily_scripts_movie_list.json')
    df_daily_scripts.to_json(json_path_daily_scripts, orient='records', lines=False)

    print('------------')
    #print(movie_list_name)
    #print('------------')
    print(f"Number of movies found (DailyScripts): {len(df_daily_scripts)}")


def get_existing_infos_imsdb_dailyscripts() :
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

def clean_str_imsdb(string) :
    try :
        partie_avant, _ = string.rsplit(", The", 1)
        modifified_string = f"The {partie_avant}"
        return modifified_string
    except :
        return string
    

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




##### Stats part
from pandasql import sqldf

pysqldf = lambda q: sqldf(q, globals())

## Error stats
# Count of error where a solution exists in DailyScripts
query_count_errors_recovarable = """
SELECT COUNT(*) as nb_error_recoverable 
FROM df_compare_error
WHERE _merge = 'both' and error IS NOT NULL;"""

# Count of error where a solution doesn't exist in DailyScripts
query_count_errors_not_recovarable = """
SELECT COUNT(*) as nb_error_not_recoverable 
FROM df_compare_error
WHERE _merge = 'left_only' and error IS NOT NULL;"""

# Count of the total numbers of errors
query_count_total_errors = """
SELECT COUNT(*) 
FROM df_compare_error
WHERE error IS NOT NULL;"""

## Duplicates and exclusive scripts
# count the duplicate between the 2 data sources
query_count_duplicates_imsdb_dailyscripts = """
SELECT COUNT(*) as nb_scripts_imsdb 
FROM df_compare_error
WHERE _merge = 'both' and error IS NULL;"""

# count exclusive to DailyScripts
query_count_exclusive_dailyscripts = """
SELECT COUNT(*) as nb_scripts_dailyscripts 
FROM df_compare_error
WHERE _merge = 'right_only' and error IS NULL;"""

# count exclusives to ImsDB
query_count_exclusive_imsdb = """
SELECT COUNT(*) as nb_scripts_dailyscripts 
FROM df_compare_error
WHERE _merge = 'left_only' and error IS NULL;"""

# count scripts scrapped from ImsDB
query_count_scrapped_from_imsdb = """
SELECT COUNT(*) as nb_scripts_dailyscripts 
FROM df_compare_error
WHERE (_merge = 'left_only' OR _merge = 'both') AND error IS NULL;"""

# count the total number of scripts in the table
query_count_all = """
SELECT COUNT(*) as total_count
FROM df_compare_error;"""

# count the total number of error where the extension doesn't need an OCR (html, htm, txt)
query_count_not_OCR_recovarable_errors = """
SELECT COUNT(*) as nb_html_recovarable_errors 
FROM df_compare_error
WHERE movie_url_extension != 'pdf' AND movie_url_extension != 'doc' AND _merge = 'both' AND error IS NOT NULL;"""


# Count the exclusive dailyScripts which aren't necessitate an OCR (html, htm, txt) - including errors from imsdb (9)
query_count_exclusive_dailyscripts_not_OCR = """
SELECT COUNT(*) as nb_exclusive_dailyscripts_not_OCR 
FROM df_compare_error
WHERE movie_url_extension != 'pdf' AND movie_url_extension != 'doc' AND (_merge = 'right_only' OR (_merge = 'both' AND error IS NOT NULL));"""

# Count the exclusive dailyScripts which necessitate an OCR (pdf, doc) - including errors from imsdb (20)
query_count_exclusive_dailyscripts_OCR = """
SELECT COUNT(*) as nb_exclusive_dailyscripts_OCR
FROM df_compare_error
WHERE (movie_url_extension = 'pdf' OR movie_url_extension = 'doc') AND (_merge = 'right_only' OR (_merge = 'both' AND error IS NOT NULL));"""


result_recoverable = pysqldf(query_count_errors_recovarable)
result_not_recoverable = pysqldf(query_count_errors_not_recovarable)
result_total_errors = pysqldf(query_count_total_errors)
result_duplicates = pysqldf(query_count_duplicates_imsdb_dailyscripts)
results_exclusive_dailyscripts = pysqldf(query_count_exclusive_dailyscripts)
results_exclusive_imsdb = pysqldf(query_count_exclusive_imsdb)
results_count_scrapped_from_imsdb = pysqldf(query_count_scrapped_from_imsdb)
results_count_all = pysqldf(query_count_all)
results_count_not_OCR_recovarable_errors = pysqldf(query_count_not_OCR_recovarable_errors)
results_count_exclusive_dailyscripts_not_OCR = pysqldf(query_count_exclusive_dailyscripts_not_OCR)  
results_count_exclusive_dailyscripts_OCR = pysqldf(query_count_exclusive_dailyscripts_OCR)


print("\n------------\n"
      f"Number of errors recoverable thanks to DailyScripts: {result_recoverable['nb_error_recoverable'][0]}\n"                         # 29
      f"Number of errors NOT recoverable: {result_not_recoverable['nb_error_not_recoverable'][0]}\n"                                    # 44
      f"Total number of errors in ImsDB: {result_total_errors.iloc[0,0]}\n"                                                             # 73
      f"Number of duplicate scripts between ImsDB and DailyScripts: {result_duplicates.iloc[0,0]}\n"                                    # 541
      f"Number of scripts exclusive to DailyScripts: {results_exclusive_dailyscripts.iloc[0,0]}\n"                                      # 522
      f"Number of scripts exclusive to ImsDB: {results_exclusive_imsdb.iloc[0,0]}\n"                                                    # 683
      f"Number of scripts scrapped from ImsDB: {results_count_scrapped_from_imsdb.iloc[0,0]}\n"                                         # 1221
      f"Total number of scripts in the table: {results_count_all.iloc[0,0]}\n"                                                          # 1819
      f"Number of errors recoverable thanks to DailyScripts (htm, html, txt): {results_count_not_OCR_recovarable_errors.iloc[0,0]}\n"   # 9
      f"Number of scripts exclusive to DailyScripts (htm, html, txt): {results_count_exclusive_dailyscripts_not_OCR.iloc[0,0]}\n"       # 232 (223 without imsdb errors)
      f"Number of scripts exclusive to DailyScripts (pdf, doc): {results_count_exclusive_dailyscripts_OCR.iloc[0,0]}\n"                 # 319 (299 without imsdb errors)
      "------------\n")


## Other queries
# errors details
# query_select_all_errors = """
# SELECT *
# FROM df_compare_error
# WHERE _merge = 'both' AND error IS NOT NULL;"""

# results_select_all_errors = pysqldf(query_select_all_errors)
# print(results_select_all_errors)
# print("\n------------\n")


# extension details - errors
query_select_extension_details = """
SELECT movie_url_extension, COUNT(movie_url_extension) as nb_extension
FROM df_compare_error
WHERE movie_url_extension IS NOT NULL
GROUP BY movie_url_extension
ORDER BY nb_extension DESC;"""
results_extension_details = pysqldf(query_select_extension_details)
print ("Extensions details:")
print(results_extension_details)

print("\n------------\n")


# extension details - errors
query_select_extension_details_errors = """
SELECT movie_url_extension, COUNT(movie_url_extension) as nb_extension
FROM df_compare_error
WHERE movie_url_extension IS NOT NULL AND error IS NOT NULL
GROUP BY movie_url_extension
ORDER BY nb_extension DESC;"""
results_extension_details_errors = pysqldf(query_select_extension_details_errors)
print ("Errors extensions details:")
print(results_extension_details_errors)

print("\n------------\n")