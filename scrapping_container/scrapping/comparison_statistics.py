import os
import pandas as pd
import re
import numpy as np

base_dir = os.path.dirname(os.path.abspath(__file__))
DATA_STATISTICS_FOLDER = os.path.join(base_dir, "data_scrapping", "statistics")


def get_existing_infos_imsdb_dailyscripts() :
    json_path_name_url_imsDB = os.path.join(DATA_STATISTICS_FOLDER, 'name_url_imsdb.json')
    json_path_error_imsDB = os.path.join(DATA_STATISTICS_FOLDER, 'error_imsdb.json')
    json_path_daily_scripts = os.path.join(DATA_STATISTICS_FOLDER, 'daily_scripts_movie_list.json')
    csv_path_scripts_slug = os.path.join(DATA_STATISTICS_FOLDER, "scripts_slug_movie_script_page_urls.csv")

    # Use pandas to load the JSON into a DataFrame (will raise if file missing)
    df_name_url_imsdb = pd.read_json(json_path_name_url_imsDB)

    # same for the error file
    df_error_name_url_imsdb = pd.read_json(json_path_error_imsDB)
    
    # same for the dailyScripts file 
    df_name_daily_scripts = pd.read_json(json_path_daily_scripts)

    # read the csv file to import scripts slug data
    df_scripts_slug_data = pd.read_csv(csv_path_scripts_slug, delimiter=",")


    return df_name_url_imsdb, df_error_name_url_imsdb, df_name_daily_scripts, df_scripts_slug_data



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
    
def apply_naming_convention(string) :
    string = string.replace(",", "")
    string = string.replace(":", "")
    string = string.replace(";", "")
    string = string.replace("'", "")
    string = string.replace(".", "")
    string = string.replace("!", "")
    string = string.replace("?", "")
    string = string.replace("IX", "9")
    string = string.replace("VIII", "8")
    string = string.replace("VII", "7")
    string = string.replace("VI", "6")
    string = string.replace("IV", "4")
    string = string.replace("III", "3")
    string = string.replace("II", "2")
    words = re.split(r'[\s_-]+', string)
    
    new_string = ' '.join(word.capitalize() for word in words)
    #new_string = new_string.lower()
    return new_string


# load the dataframes
df_name_url_imsdb, df_error_name_url_imsdb, df_name_daily_scripts, df_scripts_slug_data = get_existing_infos_imsdb_dailyscripts()


# cleaning before the merge
df_name_url_imsdb['movie_name_imsdb'] = df_name_url_imsdb['name'].apply(clean_str_imsdb)
df_name_url_imsdb['movie_name_imsdb_conventioned'] = df_name_url_imsdb['movie_name_imsdb'].apply(apply_naming_convention)
df_name_url_imsdb['full_url_imsdb'] = df_name_url_imsdb['url'].apply(lambda url: f"https://imsdb.com//scripts//{url}.html")
df_name_url_imsdb.drop(columns=['url', 'name', 'movie_name_imsdb'], inplace=True) 


df_error_name_url_imsdb['movie_name_imsdb'] = df_error_name_url_imsdb['name'].apply(clean_str_imsdb)
df_error_name_url_imsdb.insert(0, 'movie_name_imsdb_error', df_error_name_url_imsdb['movie_name_imsdb'])
df_error_name_url_imsdb.drop(columns=['movie_name_imsdb', 'url', 'name', 'movie_name_imsdb'], inplace=True) 
df_error_name_url_imsdb['movie_name_imsdb_conventioned_error'] = df_error_name_url_imsdb['movie_name_imsdb_error'].apply(apply_naming_convention)


df_name_daily_scripts['movie_url_extension'] = df_name_daily_scripts['movie_url'].apply(get_url_extension_dailyscripts)
df_name_daily_scripts['movie_name_dailyscripts_conventioned'] = df_name_daily_scripts['movie_name'].apply(apply_naming_convention)
# condition over the extension of the files : avoid to have erroneous rows (link of information page instead of script)
acceptable_extensions = ['pdf', 'html', 'htm', 'txt', 'doc', 'docx', '']
df_name_daily_scripts = df_name_daily_scripts[df_name_daily_scripts['movie_url_extension'].isin(acceptable_extensions)]  


df_scripts_slug_data['movie_name_script_slug_conventioned'] = df_scripts_slug_data['movie_name'].apply(apply_naming_convention)
df_scripts_slug_data = df_scripts_slug_data.drop(['movie_name', 'script_page_url'], axis=1)


# Merge all the cleaned previous dataframes
df_imsdb_scriptslug = pd.merge(df_scripts_slug_data, df_name_url_imsdb, left_on='movie_name_script_slug_conventioned', right_on='movie_name_imsdb_conventioned', how='outer', indicator='_merge_imsdb_scriptslug')
df_imsdb_scriptslug['movie_name_conventioned'] = np.where(
    df_imsdb_scriptslug['_merge_imsdb_scriptslug'] != 'right_only', 
    df_imsdb_scriptslug['movie_name_script_slug_conventioned'], 
    df_imsdb_scriptslug['movie_name_imsdb_conventioned'])

df_imsdb_scriptslug_error = pd.merge(df_imsdb_scriptslug, df_error_name_url_imsdb, left_on='movie_name_conventioned', right_on='movie_name_imsdb_conventioned_error', how='outer')


df_compare_error = pd.merge(df_imsdb_scriptslug_error, df_name_daily_scripts, left_on='movie_name_conventioned', right_on='movie_name_dailyscripts_conventioned', how='outer', indicator='_merge_main_dailyscripts')


#### Clean dataset for scrapping (imsDB + ScriptsSlug) ####
# URL column creation for scrapping
df_imsdb_scriptslug_error['scrapping_url'] = np.where(
    df_imsdb_scriptslug_error['_merge_imsdb_scriptslug'] != 'right_only', 
    df_imsdb_scriptslug_error['pdf_url'], 
    df_imsdb_scriptslug_error['full_url_imsdb'])

# URL extension column creation for scrapping
df_imsdb_scriptslug_error['scrapping_url_extension'] = np.where(
    df_imsdb_scriptslug_error['_merge_imsdb_scriptslug'] != 'right_only', 
    'pdf', 
    'html')


# Remove rows with error in imsdb scrapping (that can't be retrieved from scriptslug)
# Condition of filtering : keep rows where the movie is present in scriptslug or no error in imsdb scrapping
df_imsdb_scriptslug_error = df_imsdb_scriptslug_error[(df_imsdb_scriptslug_error['_merge_imsdb_scriptslug'] != 'right_only') | (df_imsdb_scriptslug_error['movie_name_imsdb_conventioned_error'].isnull())]
df_final_scrapping = df_imsdb_scriptslug_error[['movie_name_conventioned', 'scrapping_url', 'scrapping_url_extension']]

# file creation
csv_path_df_compare_full = os.path.join(DATA_STATISTICS_FOLDER, 'df_comparison_error_full.csv')
df_compare_error.to_csv(csv_path_df_compare_full, index=False)

csv_path_df_imsdb_scriptslug_full = os.path.join(DATA_STATISTICS_FOLDER, 'df_final_scrapping.csv')
df_final_scrapping.to_csv(csv_path_df_imsdb_scriptslug_full, index=False)



df_compare_light = df_compare_error[['movie_name_script_slug_conventioned', 'movie_name_imsdb_conventioned', '_merge_imsdb_scriptslug', 'movie_name_conventioned', 'movie_name_dailyscripts_conventioned', '_merge_main_dailyscripts']]

csv_path_df_compare_light = os.path.join(DATA_STATISTICS_FOLDER, 'df_comparison_error_light.csv')
df_compare_light.to_csv(csv_path_df_compare_light, index=False)





########################################### Statistics generation ###########################################
from pandasql import sqldf

pysqldf = lambda q: sqldf(q, globals())


print("\n############# Statistics about the comparison between IMSDB, DailyScripts and ScriptsSlug #############\n")

# Count of movies present in imsdb and not in scriptslug
query_imsdb_only = """
SELECT COUNT(*) AS count_imsdb_only
FROM df_compare_error
WHERE _merge_imsdb_scriptslug = 'right_only'
"""
result_imsdb_only = pysqldf(query_imsdb_only)
print(f"Count of movies present in IMSDB only: {result_imsdb_only['count_imsdb_only'][0]}")


# Count of movies present in dailyscripts only
query_dailyscripts_only = """
SELECT COUNT(*) AS count_dailyscripts_only
FROM df_compare_error
WHERE _merge_main_dailyscripts = 'right_only'
"""

result_dailyscripts_only = pysqldf(query_dailyscripts_only)
print(f"Count of movies present in DailyScripts only: {result_dailyscripts_only['count_dailyscripts_only'][0]}")


# Count of movies present in dailyscripts only with a pdf extension
query_dailyscripts_pdf_only = """
SELECT COUNT(*) AS count_dailyscripts_only
FROM df_compare_error
WHERE _merge_main_dailyscripts = 'right_only' and movie_url_extension = 'pdf'
"""

result_dailyscripts_pdf_only = pysqldf(query_dailyscripts_pdf_only)
print(f"Count of pdf scripts present in DailyScripts only: {result_dailyscripts_pdf_only['count_dailyscripts_only'][0]}, (knowing that it may have some double with other DB, due to weird name choices...)")


# count of errors in imsdb scrapping
query_count_errors_imsdb = """
SELECT COUNT(*) AS count_errors_imsdb
FROM df_compare_error
WHERE movie_name_imsdb_conventioned_error IS NOT NULL
"""
result_count_errors_imsdb = pysqldf(query_count_errors_imsdb)
print(f"Count of errors in IMSDB scrapping: {result_count_errors_imsdb['count_errors_imsdb'][0]}")



# count of errors in imsdb scrapping
query_count_errors_imsdb_covered_scriptslug = """
SELECT COUNT(*) AS count_errors_imsdb
FROM df_compare_error
WHERE (movie_name_imsdb_conventioned_error IS NOT NULL) AND (_merge_imsdb_scriptslug = 'both')
"""
result_count_errors_imsdb_covered_scriptslug = pysqldf(query_count_errors_imsdb_covered_scriptslug)
print(f"Count of errors in IMSDB scrapping: {result_count_errors_imsdb_covered_scriptslug['count_errors_imsdb'][0]}")


# count total exploitable scripts from scriptslug and imsdb
query_count_exploitable_scripts_scriptslug_imsdb = """
SELECT COUNT(*) AS exploitable_scripts_count
FROM df_compare_error
WHERE _merge_imsdb_scriptslug IS NOT NULL
"""
result_count_exploitable_scripts_scriptslug_imsdb = pysqldf(query_count_exploitable_scripts_scriptslug_imsdb)
print(f"Count of exploitable scripts (without error) in IMSDB + Scripts_slug: {result_count_exploitable_scripts_scriptslug_imsdb['exploitable_scripts_count'][0]}")


print(f"Count of exploitable scripts (with error) in IMSDB + Scripts_slug: {result_count_exploitable_scripts_scriptslug_imsdb['exploitable_scripts_count'][0] - result_count_errors_imsdb_covered_scriptslug['count_errors_imsdb'][0]}")


print("\n############# End of statistics #############\n")