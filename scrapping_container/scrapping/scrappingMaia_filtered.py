#Imports
import os
import re
import json
import time
import random
import logging
from typing import List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, NavigableString, Tag
from selenium import webdriver
from selenium.webdriver.common.by import By

#  Config : request package ?
BASE = "https://imsdb.com"
REQUEST_TIMEOUT = 20
MIN_DELAY_S = 0.6
MAX_DELAY_S = 1.2
RETRY_COUNT = 3 # in case of error during requesting : number of retry allowed for one request.
RETRY_BACKOFF = 1.8 # in case of error during requesting : number of second between two retry.


# to check if the folder exists in the volume  and to configure file outputs 
DATA_FOLDER = ".//data_scrapping"
DATA_FOLDER_SCRIPTS = f"{DATA_FOLDER}//scripts//imsDB"
DATA_FOLDER_LOGS = f"{DATA_FOLDER}//logs//imsDB"
os.makedirs(DATA_FOLDER_SCRIPTS, exist_ok=True)
os.makedirs(DATA_FOLDER_LOGS, exist_ok=True)

#  Logging 
logger = logging.getLogger()
logger.setLevel(logging.INFO)

fh = logging.FileHandler(
    f"{DATA_FOLDER_LOGS}//scrapping.log", mode="w", encoding="utf-8"
)
fh.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s")
fh.setFormatter(fmt)
ch.setFormatter(fmt)

logger.handlers = [fh, ch]
logging.info("Scraper started")

#  HTTP 
SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0 Safari/537.36"
        )
    }
)

# to make the program sleep for random time bounded by MIN_DELAY_S and MAX_DELAY_S
def _sleep_jitter(): # useful ?
    time.sleep(random.uniform(MIN_DELAY_S, MAX_DELAY_S))

def _fetch_html(url: str) -> str: # useful
    last_error = None
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            response = SESSION.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            response.raise_for_status() # raise an error if the request failed 
            return response.text
        except Exception as e:
            last_error = e
            wait = (RETRY_BACKOFF ** (attempt - 1)) + random.random() # to have a random wiating time based on the RETRY_BACKOFF variable
            logging.warning(
                f"[{attempt}/{RETRY_COUNT}] GET failed {url}: {e} | retry in {wait:.1f}s"
            )
            time.sleep(wait)
    raise last_error  # type: ignore

def _fetch_soup(url: str) -> BeautifulSoup: # useful ?
    _sleep_jitter()
    return BeautifulSoup(_fetch_html(url), "html.parser")

#  Utils 
def _norm(txt: str) -> str:
    if not txt:
        return ""
    txt = txt.replace("\xa0", " ").replace("&nbsp;", " ")
    txt = re.sub(r"\s+", " ", txt)
    return txt.strip()

def _safe_filename(name: str) -> str:
    name = (name or "untitled").strip()
    name = re.sub(r"[^\w\-.]+", "_", name)
    return name[:180]

#  Selenium driver 
def _build_driver() -> Optional[webdriver.Chrome]:
    try:
        opts = webdriver.ChromeOptions()
        opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument(
            "user-agent=Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0 Safari/537.36"
        )
        driver = webdriver.Chrome(options=opts)
        driver.set_window_rect(0, 0, 1280, 900)
        driver.implicitly_wait(5)
        logging.info("Selenium driver initialized")
        return driver
    except Exception as e:
        logging.warning(f"Selenium not available, fallback to requests only: {e}")
        return None

DRIVER = _build_driver()





#  Script body struct 
def _structure_html_to_json(html_script: str, url: str) -> dict:
    soup = BeautifulSoup(html_script, "html.parser")
    elements = []
    for content in soup.contents:
        if getattr(content, "name", None):
            elements.append({
                "type": "tag",
                "name": content.name,
                "content": str(content)
            })
        else:
            text = str(content).strip()
            if text:
                elements.append({"type": "text", "content": text})
    return {"url": url, "elements": elements}






def _find_script_link_from_movie_page(soup: BeautifulSoup, movie_url: str) -> Optional[str]:
    anchors = soup.find_all("a", href=True)

    # 1) "Read ..." links
    for a in anchors:
        if _norm(a.get_text()).lower().startswith("read "):
            return urljoin(movie_url, a["href"])

    # 2) /scripts/ links
    for a in anchors:
        if "/scripts/" in a["href"]:
            return urljoin(BASE, a["href"])

    # 3) fallback: href mentioning script/screenplay
    for a in anchors:
        href = a["href"].lower()
        if "script" in href or "screenplay" in href:
            return urljoin(movie_url, a["href"])

    return None

def _is_probably_pdf(url: str) -> bool:
    return urlparse(url).path.lower().endswith(".pdf") or ".pdf" in urlparse(url).path.lower()

#  Scrape one script 
def _get_script(movie_scripts_url: str):
    ms_soup = _fetch_soup(movie_scripts_url)

    title = _extract_title(ms_soup)
    genres = _extract_genres(ms_soup)
    rating = _extract_user_rating(ms_soup)

    script_url = _find_script_link_from_movie_page(ms_soup, movie_scripts_url)
    script_html = ""
    script_text = ""
    structured = None
    note = None

    if not script_url:
        raise Exception(f"No script link found on {movie_scripts_url}")

    if _is_probably_pdf(script_url) or (
        urlparse(script_url).netloc
        and urlparse(script_url).netloc != urlparse(BASE).netloc
    ):
        note = f"external_or_pdf: {script_url}"
        logging.info(f"[external] {movie_scripts_url} -> {script_url}")
    else:
        # try Selenium
        if DRIVER is not None:
            try:
                DRIVER.get(script_url)
                time.sleep(0.6)
                try:
                    html_script = DRIVER.find_element(
                        By.XPATH,
                        "//*[@id='mainbody']/table[2]//td[3]//table//td/pre",
                    ).get_attribute("innerHTML")
                except Exception:
                    html_script = DRIVER.find_element(
                        By.XPATH,
                        "//*[@id='mainbody']/table[2]//td[3]//table//td",
                    ).get_attribute("innerHTML")
                html_script = html_script.replace("<pre>", "").replace("</pre>", "")
                script_html = html_script
            except Exception as e:
                logging.warning(f"Selenium script fetch failed ({script_url}): {e}")

        # fallback requests
        if not script_html:
            sc_soup = _fetch_soup(script_url)
            pre = sc_soup.find("pre")
            if pre:
                script_html = pre.decode()
            else:
                td = sc_soup.select_one(
                    "#mainbody table:nth-of-type(2) td:nth-of-type(3) table td"
                )
                if td:
                    script_html = td.decode()

    if script_html:
        text_soup = BeautifulSoup(script_html, "html.parser")
        script_text = _norm(text_soup.get_text("\n"))
        structured = _structure_html_to_json(script_html, script_url)

    # Filename from script_url or movie_scripts_url
    source = script_url or movie_scripts_url or title
    slug = os.path.basename(urlparse(source).path).replace(".html", "") or "untitled"
    filename = _safe_filename(slug)

    data = {
        "title": title,
        "metadata": {
            "genres": genres,
            "user_rating": rating,
        },
        "script": {
            "url": script_url,
            "html": script_html,
            "text": script_text,
            "structured_json": structured,
            "note": note,
        }
    }

    path = os.path.join(DATA_FOLDER_SCRIPTS, f"{filename}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    logging.info(f"[OK] {movie_scripts_url} -> {filename}.json")

#  Loop all 
def _get_all_scripts(dict_url_list: dict):
    for letter, url_list in dict_url_list.items():
        logging.info(f"== {letter} : {len(url_list)} Movie Scripts URLs ==")
        for url in url_list:
            try:
                _get_script(url)
            except Exception as e:
                logging.error(f"[FAIL] {url} | {e}")
    logging.info("All scripts processed")

#  Main 
def main():
    _get_all_scripts(dict_url_list)
    logging.info("Scraping completed")

if __name__ == "__main__":
    try:
        main()
    finally:
        try:
            if DRIVER is not None:
                DRIVER.quit()
        except Exception:
            pass
