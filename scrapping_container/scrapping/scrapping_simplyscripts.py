import requests
import logging
import re
import os
import json
from io import BytesIO
import PyPDF2
from PyPDF2.errors import PdfReadError
import docx
from bs4 import BeautifulSoup

### --Initialization--
DATA_FOLDER = ".//data_scrapping"
#DATA_FOLDER_SCRIPTS = f"{DATA_FOLDER}//scripts"
DATA_FOLDER_SCRIPTS = f"{DATA_FOLDER}//scripts//simplyScripts"
#DATA_FOLDER_LOGS = f"{DATA_FOLDER}//logs"
DATA_FOLDER_LOGS = f"{DATA_FOLDER}//logs//simplyScripts"

# Error file path
ERROR_FILE = os.path.join(DATA_FOLDER, "SimplyScriptsDB_scrapping_error.txt")

# Allowed file extensions for scripts
ALLOWED_EXTENSIONS = [".pdf", ".html", ".txt", ".doc", ".docx"]


# logging 
logging.basicConfig(
    filename=f"{DATA_FOLDER_LOGS}//scrapping.log",
    filemode='w',
    level=logging.INFO
    )
logging.info("Scrapper runner started")

### TOOLS ###
def _initialize_alpha_index():
    """
    Returns a list of alphabetical indexes for SimplyScripts.
    'num' covers scripts starting with a number,
    then a-u, then special indexes 'uvw' and 'xyz'.
    """
    alphabetical_index = ["num"] + [chr(i) for i in range(97, 117)] + ["uvw", "xyz"]
    return alphabetical_index

def _url_valid(url: str):
    """
    Check if a URL is valid by sending a HEAD request.
    Returns True if the URL responds with status code 200.
    """
    if not url or not url.startswith("http"):
        return False
    try:
        response = requests.head(url, timeout=10, allow_redirects=True)
        return response.status_code == 200
    except requests.RequestException:
        return False

def _get_url_script(alpha_index: str):
    """
    Scrape SimplyScripts for a given alphabetical index.
    Returns a list of dictionaries containing:
        - title
        - script URL
        - IMDb ID
    """
    url = f"https://www.simplyscripts.com/{alpha_index}.html"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
    except requests.RequestException:
        logging.info(f"[!] Failed to fetch page: {url}")
        return []

    # Parse HTML using BeautifulSoup
    soup = BeautifulSoup(response.text, "html.parser")
    table_rows = soup.select("div#movie_wide table tr")
    script_list = []
    seen_titles = set()

    for row in table_rows:
        try:
            # Get script title link and IMDb link
            script_link = row.select_one("td:nth-child(1) a")
            imdb_link = row.select_one("td:nth-child(5) a")
            if not script_link or not imdb_link:
                continue

            title = script_link.get_text(strip=True)
            url_script = script_link.get("href")
            url_id = imdb_link.get("href")

            if not _url_valid(url_script):
                continue

            match = re.search(r"(tt\d+)", url_id)
            if not match:
                continue
            imdb_id = match.group(1)

            if title.lower() in seen_titles:
                continue
            seen_titles.add(title.lower())

            # Append the data dictionary
            script_list.append({
                "title": title,
                "url_script": url_script,
                "imdb_id": imdb_id
            })

        except Exception:
            continue

    return script_list

def _get_movie_data_omdb(imdb_id: str, api_key: str):
    """
    Fetch movie metadata from OMDb API using IMDb ID.
    Returns a dictionary with title, genre, and IMDb rating.
    """
    url = f"http://www.omdbapi.com/?i={imdb_id}&apikey={api_key}"
    try:
        response = requests.get(url, timeout=10)
        data = response.json()
        if data.get("Response") == "True":
            return {
                "title": data.get("Title"),
                "genre": data.get("Genre"),
                "rating": data.get("imdbRating"),
            }
        else:
            logging.info(f"[!] OMDb error: {data.get('Error')}")
    except requests.RequestException:
        logging.info(f"[!] Failed to call OMDb API for {imdb_id}")
    return None

def merge_single_script_with_movie_data(script, movie_data):
    """
    Merge script metadata and OMDb movie data into a single dictionary.
    """
    if not script or not movie_data:
        return None
    return {
        "title": movie_data.get("title") or script.get("title"),
        "url_script": script.get("url_script"),
        "genre": movie_data.get("genre"),
        "rating": movie_data.get("rating"),
    }

def _add_to_missing(title, url, reason="Empty or corrupted"):
    """
    Log missing, empty, or corrupted scripts into a text file.
    Creates the folder and the error file automatically if they don't exist.
    """

    # Ensure the folder exists
    os.makedirs(DATA_FOLDER, exist_ok=True)

    # Ensure the file exists (creates it if missing)
    if not os.path.exists(ERROR_FILE):
        with open(ERROR_FILE, "w", encoding="utf-8") as f:
            f.write("=== SimplyScripts Scraping Errors ===\n\n")

    # Append the new error entry
    with open(ERROR_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{reason}] - {title} ({url})\n")

    logging.info(f"[!] Logged error for: {title} ({reason})")


def download_script_content(script_info):
    """
    Download the script from its URL and attach its text content to the dictionary.
    Supports .txt, .html, .pdf, and .docx.
    Logs empty or corrupted scripts using _add_to_missing.
    """
    url = script_info.get("url_script")
    title = script_info.get("title", "unknown_title")

    if not url:
        return None

    ext = os.path.splitext(url)[1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        logging.info(f"[!] Skipping unsupported file type: {url}")
        return None

    script_text = ""
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        content_bytes = response.content

        # ---- TXT or HTML ----
        if ext in [".txt", ".html"]:
            script_text = content_bytes.decode("utf-8", errors="ignore").replace("\n", " ")

        # ---- PDF ----
        elif ext == ".pdf":
            try:
                pdf_reader = PyPDF2.PdfReader(BytesIO(content_bytes))
                for page in pdf_reader.pages:
                    text = page.extract_text()
                    if text:
                        script_text += text.replace("\n", " ")
            except PdfReadError:
                logging.info(f"[!] Corrupted PDF: {url}")
                _add_to_missing(title, url, "PDF corrupted")

        # ---- DOCX ----
        elif ext == ".docx":
            try:
                doc = docx.Document(BytesIO(content_bytes))
                for para in doc.paragraphs:
                    script_text += para.text + " "
            except Exception:
                logging.info(f"[!] Failed to read DOCX: {url}")
                _add_to_missing(title, url, "DOCX read error")

    except requests.RequestException:
        logging.info(f"[!] Request failed for {url}")
        _add_to_missing(title, url, "Request failed")

    # ---- Check empty content ----
    if not script_text.strip():
        _add_to_missing(title, url, "Empty script")

    script_info["script"] = script_text.strip()
    return script_info

### MAIN EXECUTION ###
if __name__ == "__main__":

    # Get all alphabetical indexes
    alphabetical_indexes = _initialize_alpha_index()
    api_key = "2995ec4a"

    # Ensure output folder exists
    os.makedirs(DATA_FOLDER_SCRIPTS, exist_ok=True)

    # Loop through each alphabetical index
    for alpha_index in alphabetical_indexes:
        logging.info(f"[+] Scraping index: {alpha_index}")

        # Scrape all scripts URLs for this index
        list_scripts_url = _get_url_script(alpha_index)
        all_data = []

        # Fetch movie metadata for each script
        for script in list_scripts_url:
            imdb_id = script.get("imdb_id")
            movie_data = _get_movie_data_omdb(imdb_id, api_key)
            if movie_data:
                all_data.append(merge_single_script_with_movie_data(script, movie_data))

        # Download script content and save each movie as a JSON file
        for script_info in all_data:
            updated_info = download_script_content(script_info)
            if not updated_info:
                continue

            title_clean = updated_info["title"].replace(" ", "_").replace("/", "_")
            output_path = os.path.join(DATA_FOLDER_SCRIPTS, f"{title_clean}.json")

            # Save the JSON file
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(updated_info, f, ensure_ascii=False, indent=4)

            logging.info(f"[+] Saved: {output_path}")

    logging.info("\n[+] Scraping completed!")



