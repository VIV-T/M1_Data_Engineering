### Imports
# Diverse tools
import requests
import json
from bs4 import BeautifulSoup
import logging
import pandas as pd

# Selenium : web navigation and scrapping
from selenium import webdriver
from selenium.webdriver.common.by import By




### --Initialization--
DATA_FOLDER = ".//data_scrapping"
DATA_FOLDER_SCRIPTS = f"{DATA_FOLDER}//scripts"
DATA_FOLDER_LOGS = f"{DATA_FOLDER}//logs"

# logging 
logging.basicConfig(
    filename=f"{DATA_FOLDER_LOGS}//scrapping.log",
    filemode='w',
    level=logging.INFO
    )
logging.info("Scrapper runner started")

# Selenium driver
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--verbose')

DRIVER = webdriver.Chrome(chrome_options)
DRIVER.set_window_rect(0,0,1280,840)

logging.info("Driver set properly")



###--Tools--
# Create the alphabetical index (based on the website structure)
def _initialize_alpha_index():
    alphabetical_index = []
    # index 0
    alphabetical_index.append(str(0))
    # All the MAJ letter
    for i in range(65, 91):
        alphabetical_index.append(chr(i))

    logging.info("Alpha index initialized")
    return alphabetical_index


# get the name_list and url_list to next scrapp the ressources
def _get_name_url_list(alphabetical_index : str, url : bool = False) : 
    # go to the website url
    DRIVER.get(f'https://imsdb.com/alphabetical/{alphabetical_index}')
    
    # find the namelist base on the alphabetical index
    web_elem_list = DRIVER.find_elements(by="xpath", value="//*[@id='mainbody']/table[2]/tbody/tr/td[3]//a")
    name_list = list(map(lambda elem : elem.text, web_elem_list))

    # build 'url_list' based on 'name_list' (cf. url structure on the website - html ressources) 
    if url :
        name_list =  list(map(lambda name : name.replace(": ", "-"), name_list))
        name_list =  list(map(lambda name : name.replace("%", "%2526"), name_list))
        name_list =  list(map(lambda name : name.replace(" ", "-"), name_list))
    
    return name_list


# Loop on each index (letter of the alphabet + 0 -- cf. the website structure)
def _get_dict_name_url_list(alphabetical_index : list, url : bool = False) :

    # loop
    dict_name_url_list = dict()
    for alpha_index in alphabetical_index : 
        dict_name_url_list[alpha_index] = _get_name_url_list(alpha_index, url)

    if url :
        logging.info("dict_url_list initialized")
    else : 
        logging.info("dict_name_list initialized")

    return dict_name_url_list


# Use BeautifulSoup to structure the data and convert it to JSON - useful for the text outside <>
def _structure_html_to_json(html_script):
    soup = BeautifulSoup(html_script, 'html.parser')

    # extract all elements and text node
    html_elements = []
    for content in soup.contents:
        if content.name:  # it's a html tag
            html_elements.append({"type": "tag", "name": content.name, "content": str(content)})
        else:  # it's text
            text = content.strip()
            if text:  # ignore empty str
                html_elements.append({"type": "text", "content": text})
    
    # convert to JSON
    json_script = json.dumps({"elements": html_elements}, ensure_ascii=False, indent=2)
    return json_script



# get the html script based on the url
def _get_script (url : str) : 
    # first check if the html ressource exist
    script_url = f"https://imsdb.com//scripts//{url}.html"
    response = requests.get(script_url)
    if response.status_code != 200 :
        raise Exception(f"Webstatus : {response.status_code}\n")
    DRIVER.get(f"https://imsdb.com//scripts//{url}.html")

    # different page structures 
    try :
        html_script = DRIVER.find_element(by=By.XPATH, value="//*[@id='mainbody']/table[2]/tbody/tr/td[3]/table/tbody/tr/td/pre").get_attribute('innerHTML')
    except :
        html_script = DRIVER.find_element(by=By.XPATH, value="//*[@id='mainbody']/table[2]/tbody/tr/td[3]/table/tbody/tr/td").get_attribute('innerHTML')
    
    # sometimes, you can scrapped empty content.
    if len(html_script) == 0 :
        raise Exception("HTML len = 0 : no content scrapped")
    
    # First cleaning of the html structure -> can be anoying when trying to use BeautifulSoup
    html_script = html_script.replace("<pre>", "")
    html_script = html_script.replace("</pre>", "")

    # use BeautifulSoup to structure the data and convert it to JSON - useful for the text outside <>
    json_script = _structure_html_to_json(html_script=html_script)
    
    # write data inside json file
    with open(f"{DATA_FOLDER_SCRIPTS}//{url}.json", "w", encoding='utf-8') as f :
        f.write(json_script)

    logging.info(f"Script scrapped and written : {url}.json")

    return True


# iteration on the alphabetical_index (url_list) to get all the html ressources and build the json files
def _get_all_scripts(dict_url_list : dict) :
    # iterations
    for url_list in dict_url_list.values() :
        for url in url_list :
            try :
                _get_script(url=url)
            # in case of Execption, print it in a dedicated file
            except Exception as e :
                # with open(f"{DATA_FOLDER}//0_error.txt", "a") as f :
                #     f.write(f"Fail : {url}      Error : {str(e)}\n")
                DICT_ERRORS["url"].append(url)
                DICT_ERRORS["error"].append(str(e))

    logging.info("All scripts scrapped")
    return True


# build a dataframe with the name and url of each script
# Useful to build the error file - based on a DataFrame merge to this one
def _build_df_name_url(dict_name_list : dict, dict_url_list : dict) :
    try :
        names_list = [name for sublist in dict_name_list.values() for name in sublist]
        urls_list = [url for sublist in dict_url_list.values() for url in sublist]
        dict_name_url = {"name" : names_list, "url" : urls_list}
        global DF_NAME_URL
        DF_NAME_URL = pd.DataFrame(dict_name_url)
        logging.info("Dataframe of name and url built")
        return True
    
    except Exception as e :
        logging.error(f"Error during the building of the dataframe : {str(e)}")
        return False
    

def _build_error_file() :
    df_error  = pd.DataFrame(DICT_ERRORS)
    # merge the two df
    last_df_error = pd.merge(DF_NAME_URL, df_error, on="url", how='inner') 
    # write the result inside a json file
    last_df_error.to_json(path_or_buf=f"{DATA_FOLDER}//SCRAPPING_ERROR.json", orient='records')
    logging.info("Error file built")  
    
    return True 



def main() :    
    # initialization
    alpha_index = _initialize_alpha_index()
    dict_name_list =_get_dict_name_url_list(alphabetical_index=alpha_index, url=False)
    dict_url_list = _get_dict_name_url_list(alphabetical_index=alpha_index, url=True)
    _build_df_name_url(dict_name_list=dict_name_list, dict_url_list=dict_url_list)
    

    global DICT_ERRORS
    DICT_ERRORS = {"url" : [], "error" : []}

    # scrapping
    #_get_script(url="12-Monkeys")
    _get_all_scripts(dict_url_list=dict_url_list)
    
    # building of the error file
    _build_error_file()




###--Main execution--
if __name__ == "__main__" :
    main()