from lxml_html_clean import Cleaner
import lxml.html
import logging
import glob
import os


# logger (based on the "main.py" script config)
logger = logging.getLogger("stagging_html_cleaning")

# Configuration
MARKER = "ALL SCRIPTS"

INGESTION_FOLDER = "./project_data/ingestion_data"
STAGGING_FOLDER = "./project_data/stagging_data"
HTML_FOLDER = os.path.join(INGESTION_FOLDER, "scripts", "html_data")


# function to build the new filepath based on the file_path.
def build_file_path(file_path : str):
    filename = os.path.splitext(os.path.basename(file_path))[0]
    new_filename = filename + ".txt"
    new_file_path = os.path.join(STAGGING_FOLDER, "scripts", new_filename)
    logger.info(f"New file path created : {new_file_path}")
    return new_file_path



def single_html_cleaning(file_path: str):

    # 1. Read HTML
    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        html = f.read()

    # 2) Clean HTML with the cleaner from lxml
    cleaner = Cleaner(
        style=True,
    )

    doc = lxml.html.fromstring(html)
    doc = cleaner.clean_html(doc)

    # 3) Get text content
    text = doc.text_content()

    # 4) Delete everything before and including "ALL SCRIPTS"
    pos = text.find(MARKER)
    if pos != -1:
        text = text[pos + len(MARKER):]

    return text



def main_stagging_html_cleaning():
    logger.info("Starting the text extraction from pdf files.")
    # Nt glob allow us to iter on all the file inside a folder.
    for html_path in glob.glob(f"{HTML_FOLDER}/*"):
       
        logger.info(f"{html_path} start the process")
        new_file_path = build_file_path(file_path=html_path)
        script_text = single_html_cleaning(file_path=html_path)
    
            
        # 5) write the script into a txt file
        with open(new_file_path, "w", encoding="utf-8") as f:
            f.write(script_text.strip() + "\n")

        print(f"{html_path} cleaned ->", new_file_path)