# Imports
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

#  Selenium driver
from selenium import webdriver
from selenium.webdriver.common.by import By

# Initialization
BASE = "https://imsdb.com"
REQUEST_TIMEOUT = 20
MIN_DELAY_S = 0.6
MAX_DELAY_S = 1.2
RETRY_COUNT = 3
RETRY_BACKOFF = 1.8

DATA_FOLDER = ".//data_scrapping"
DATA_FOLDER_SCRIPTS = f"{DATA_FOLDER}//scripts//imsDB"
DATA_FOLDER_LOGS = f"{DATA_FOLDER}//logs//imsDB"
os.makedirs(DATA_FOLDER_SCRIPTS, exist_ok=True)
os.makedirs(DATA_FOLDER_LOGS, exist_ok=True)

# To test for a single letter : SINGLE_LETTER=A
SINGLE_LETTER = os.getenv("SINGLE_LETTER", "").strip().upper()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

fh = logging.FileHandler(f"{DATA_FOLDER_LOGS}//scrapping.log", mode="w", encoding="utf-8")
fh.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s")
fh.setFormatter(fmt)
ch.setFormatter(fmt)

logger.handlers = [fh, ch]
logging.info("Scrapper runner started")

#  HTTP helpers 

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/123.0 Safari/537.36"
})

def _sleep_jitter():
    time.sleep(random.uniform(MIN_DELAY_S, MAX_DELAY_S))

def _fetch_html(url: str) -> str:
    last_err = None
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            r = SESSION.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last_err = e
            wait = (RETRY_BACKOFF ** (attempt - 1)) + random.random()
            logging.warning(f"[{attempt}/{RETRY_COUNT}] GET fail {url}: {e} | retry in {wait:.1f}s")
            time.sleep(wait)
    raise last_err  # type: ignore

def _fetch_soup(url: str) -> BeautifulSoup:
    _sleep_jitter()
    return BeautifulSoup(_fetch_html(url), "html.parser")

# -------------------- Utils parsing --------------------

def _norm(txt: str) -> str:
    if not txt:
        return ""
    txt = txt.replace("\xa0", " ").replace("&nbsp;", " ")
    txt = re.sub(r"\s+", " ", txt)
    return txt.strip()

def _safe_filename(name: str) -> str:
    name = (name or "untitled").strip()
    name = re.sub(r"[^\w\-.]+", "_", name, flags=re.UNICODE)
    return name[:180]

# Selenium driver setup

def _build_driver() -> Optional[webdriver.Chrome]:
    try:
        opts = webdriver.ChromeOptions()
        opts.add_argument('--headless=new')
        opts.add_argument('--no-sandbox')
        opts.add_argument('--disable-dev-shm-usage')
        opts.add_argument('user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/123.0 Safari/537.36')
        driver = webdriver.Chrome(options=opts)
        driver.set_window_rect(0, 0, 1280, 900)
        driver.implicitly_wait(5)
        logging.info("Selenium driver OK")
        return driver
    except Exception as e:
        logging.warning(f"Selenium driver KO, fallback requests-only. Reason: {e}")
        return None

DRIVER = _build_driver()

#  Index alphabetical 

def _initialize_alpha_index() -> List[str]:
    alpha = ['0']
    alpha += [chr(i) for i in range(65, 91)]  # A-Z
    logging.info("Alpha index initialized")
    return alpha

def _get_name_url_list(alphabetical_index: str, url: bool = False):
    target = f'{BASE}/alphabetical/{alphabetical_index}'

    urls = []
    names = []

    #  Selenium
    if DRIVER is not None:
        try:
            DRIVER.get(target)
            time.sleep(1.0)
            # XPATH 1 : without tbody
            elems = DRIVER.find_elements(By.XPATH, "//*[@id='mainbody']/table[2]//td[3]//a")
            if not elems:
                elems = DRIVER.find_elements(By.XPATH, "//*[@id='mainbody']/table[2]/tbody/tr/td[3]//a")

            if elems:
                for e in elems:
                    txt = (e.text or "").strip()
                    href = (e.get_attribute("href") or "").strip()
                    if txt:
                        names.append(txt)
                    if "/Movie%20Scripts/" in href and href.endswith(".html"):
                        urls.append(href)
        except Exception as e:
            logging.warning(f"Selenium listing failed for {target}: {e}")

    # --- Fallback requests+BS4 ---
    if not urls or (not names and not url):
        soup = _fetch_soup(target)
        anchors = soup.find_all("a", href=True)
        for a in anchors:
            href = a["href"]
            text = a.get_text(strip=True)
            if text:
                names.append(text)
            if "/Movie Scripts/" in href and href.endswith(".html"):
                urls.append(urljoin(BASE, href.replace(" ", "%20")))

        if not urls:
            # dump for debug
            dump_path = os.path.join(DATA_FOLDER_SCRIPTS, f"debug_ALPHA_{alphabetical_index}.html")
            try:
                with open(dump_path, "w", encoding="utf-8") as f:
                    f.write(str(soup))
                logging.warning(f"[alpha:{alphabetical_index}] 0 URLs — page dumpée : {dump_path}")
            except Exception as e:
                logging.warning(f"[alpha:{alphabetical_index}] dump impossible: {e}")

    def _dedup(seq):
        seen, out = set(), []
        for x in seq:
            if x not in seen:
                seen.add(x)
                out.append(x)
        return out

    names = _dedup([n for n in names if n])
    urls  = _dedup([u for u in urls if u])

    return urls if url else names

def _get_dict_name_url_list(alphabetical_index: list, url: bool = False):
    d = {}
    for letter in alphabetical_index:
        d[letter] = _get_name_url_list(letter, url=url)
    logging.info(f"dict_{'url' if url else 'name'}_list initialized")
    return d

#  HTML to structured JSON

def _structure_html_to_json(html_script: str, url: str):
    soup = BeautifulSoup(html_script, 'html.parser')
    html_elements = []
    for content in soup.contents:
        if getattr(content, "name", None):  # balise HTML
            html_elements.append({"type": "tag", "name": content.name, "content": str(content)})
        else:  # texte
            text = str(content).strip()
            if text:
                html_elements.append({"type": "text", "content": text})
    return json.dumps({'url': url, "elements": html_elements}, ensure_ascii=False, indent=2)

# Extraction of metadata/links 

def _extract_title(soup: BeautifulSoup) -> str:
    h1 = soup.find("h1")
    if h1:
        return " ".join(h1.get_text(strip=True).split())
    title = soup.find("title")
    return " ".join(title.get_text(strip=True).split()) if title else ""

def _extract_user_rating(soup: BeautifulSoup) -> Optional[float]:
    label = soup.find(lambda tag: tag.name in ("b", "strong") and tag.get_text(strip=True).lower() == "average user rating")
    if not label:
        full_txt = _norm(soup.get_text(" "))
        m = re.search(r'Average user rating[^0-9]*(\d+(?:\.\d+)?)\s*out of\s*10', full_txt, flags=re.I)
        if m:
            try:
                return float(m.group(1))
            except ValueError:
                return None
        img = soup.find("img", src=re.compile(r"/images/rating/(\d+)-stars\.gif$"))
        if img:
            m2 = re.search(r"/images/rating/(\d+)-stars\.gif$", img.get("src", ""))
            if m2:
                try:
                    return float(m2.group(1))
                except ValueError:
                    return None
        return None

    # loof for '(X out of 10)'
    for node in label.next_elements:
        if isinstance(node, NavigableString):
            txt = " ".join(str(node).split())
            m = re.search(r'(\d+(?:\.\d+)?)\s*out of\s*10', txt, flags=re.I)
            if m:
                try:
                    return float(m.group(1))
                except ValueError:
                    pass
        elif isinstance(node, Tag) and node.name in ("b", "strong"):
            break

    for node in label.next_elements:
        if isinstance(node, Tag) and node.name == "img":
            src = node.get("src", "")
            m = re.search(r'/images/rating/(\d+)-stars\.gif$', src)
            if m:
                try:
                    return float(m.group(1))
                except ValueError:
                    pass
        elif isinstance(node, Tag) and node.name in ("b", "strong"):
            break
    return None

def _extract_genres(soup: BeautifulSoup) -> List[str]:
    genres: List[str] = []
    gtag = soup.find(lambda tag: tag.name in ("b", "strong") and tag.get_text(strip=True) == "Genres")
    if gtag:
        for a in gtag.find_all_next("a"):
            href = a.get("href", "")
            if "/genre/" in href:
                genres.append(_norm(a.get_text()))
            else:
                break
    else:
        for a in soup.find_all("a", href=True):
            if "/genre/" in a["href"]:
                genres.append(_norm(a.get_text()))
    out, seen = [], set()
    for g in genres:
        if g and g not in seen:
            seen.add(g)
            out.append(g)
    return out

def _find_script_link_from_movie_page(soup: BeautifulSoup, movie_url: str) -> Tuple[Optional[str], str]:
    anchors = soup.find_all("a", href=True)
    # 1) Texte "Read ..."
    for a in anchors:
        text = _norm(a.get_text()).lower()
        if text.startswith("read "):
            return urljoin(movie_url, a["href"]), "text:Read …"
    # 2) URL /scripts/
    for a in anchors:
        if "/scripts/" in a["href"]:
            return urljoin(BASE, a["href"]), "href:/scripts/"
    # 3) Fallback 
    for a in anchors:
        text = _norm(a.get_text()).lower()
        href = a["href"].lower()
        if any(k in text for k in ("script", "screenplay", "read")) or any(k in href for k in ("script", "screenplay")):
            return urljoin(movie_url, a["href"]), "semantic"
    return None, "not-found"

def _is_probably_pdf(url: str) -> bool:
    path = urlparse(url).path.lower()
    return path.endswith(".pdf") or ".pdf" in path

#  Scraper film  

def _get_script(url: str):

    movie_scripts_url = url

    # 1) Page Movie Scripts (requests + BS4)
    ms_soup = _fetch_soup(movie_scripts_url)
    title = _extract_title(ms_soup)
    rating = _extract_user_rating(ms_soup)
    genres = _extract_genres(ms_soup)

    # 2) Link to the script page
    script_url, why = _find_script_link_from_movie_page(ms_soup, movie_scripts_url)
    script_html = ""
    script_text = ""
    structured = None
    external_note = None

    if not script_url:
        raise Exception(f"Aucun lien script trouvé ({why}) sur {movie_scripts_url}")

    # 3) Si externe/PDF → on stocke l’URL, sans extraction
    if _is_probably_pdf(script_url) or (
        urlparse(script_url).netloc and urlparse(script_url).netloc != urlparse(BASE).netloc
    ):
        external_note = f"external_or_pdf: {script_url}"
        logging.info(f"[script] externe/PDF détecté → {script_url}")
    else:
        # 4) See if Selenium works
        if DRIVER is not None:
            try:
                DRIVER.get(script_url)
                time.sleep(0.6)

                # XPATH without tbody
                try:
                    html_script = DRIVER.find_element(
                        By.XPATH, "//*[@id='mainbody']/table[2]//td[3]//table//td/pre"
                    ).get_attribute('innerHTML')
                except Exception:
                    # fallback with tbody and without <pre>
                    try:
                        html_script = DRIVER.find_element(
                            By.XPATH, "//*[@id='mainbody']/table[2]/tbody/tr/td[3]/table/tbody/tr/td/pre"
                        ).get_attribute('innerHTML')
                    except Exception:
                        html_script = DRIVER.find_element(
                            By.XPATH, "//*[@id='mainbody']/table[2]//td[3]//table//td"
                        ).get_attribute('innerHTML')

                html_script = html_script.replace("<pre>", "").replace("</pre>", "")
                script_html = html_script
                text_soup = BeautifulSoup(html_script, "html.parser")
                script_text = _norm(text_soup.get_text("\n"))
            except Exception as e:
                logging.warning(f"Selenium extract failed → fallback requests ({e})")

        # 5) Fallback requests+BS4 if Selenium doesn't work
        if not script_html:
            sc_soup = _fetch_soup(script_url)
            pre = sc_soup.find("pre")
            if pre:
                script_html = pre.decode()
                script_text = _norm(pre.get_text("\n"))
            else:
                td = sc_soup.select_one("#mainbody table:nth-of-type(2) td:nth-of-type(3) table td")
                if td:
                    script_html = td.decode()
                    script_text = _norm(td.get_text("\n"))

        if script_html:
            structured = _structure_html_to_json(script_html, url=script_url)

    # 6) Constructionof final json
    data = {
        "title": title,
        "metadata": {
            "genres": genres,
            "user_rating": rating
        },
        "script": {
            "url": script_url,
            "html": script_html,
            "text": script_text,
            "structured_json": structured,
            "note": external_note
        },
        "source_urls": {
            "movie_scripts": movie_scripts_url,
            "script": script_url
        }
    }

    # 7) Name of file
    source_for_slug = script_url or movie_scripts_url or title
    parsed = urlparse(source_for_slug)
    slug_candidate = os.path.basename(parsed.path) if parsed.path else source_for_slug
    slug_candidate = slug_candidate.replace(".html", "")
    filename = _safe_filename(slug_candidate if slug_candidate else title)

    with open(f"{DATA_FOLDER_SCRIPTS}//{filename}.json", "w", encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    logging.info(f"Script scrapped and written : {filename}.json")
    return True

# iteration on the alphabetical_index (url_list) to get all the html ressources and build the json files
def _get_all_scripts(dict_url_list: dict):
    for letter, url_list in dict_url_list.items():
        logging.info(f"== Lettre {letter} : {len(url_list)} fiches ==")
        for url in url_list:
            try:
                _get_script(url=url)  # Movie Scripts URL
            except Exception as e:
                DICT_ERRORS["url"].append(url)
                DICT_ERRORS["error"].append(str(e))
    logging.info("All scripts scrapped")
    return True


# build a dataframe with the name and url of each script
# Useful to build the error file - based on a DataFrame merge to this one
def _build_df_name_url(dict_name_list: dict, dict_url_list: dict):
    try:
        names_list = [name for sublist in dict_name_list.values() for name in sublist]
        urls_list = [url for sublist in dict_url_list.values() for url in sublist]
        dict_name_url = {"name": names_list, "url": urls_list}
        global DF_NAME_URL
        import pandas as pd
        DF_NAME_URL = pd.DataFrame(dict_name_url)
        logging.info("Dataframe of name and url built")
        return True
    except Exception as e:
        logging.error(f"Error during the building of the dataframe : {str(e)}")
        return False

def _build_error_file():
    import pandas as pd
    df_error = pd.DataFrame(DICT_ERRORS)
    last_df_error = pd.merge(DF_NAME_URL, df_error, on="url", how='inner')
    last_df_error.to_json(path_or_buf=f"{DATA_FOLDER}//imsDB_scrapping_error.json", orient='records')
    logging.info("Error file built")
    return True



def main():
    # 1) Initialization
    if SINGLE_LETTER:
        alpha_index = [SINGLE_LETTER]
        logging.info(f"Test SINGLE_LETTER={SINGLE_LETTER}")
    else:
        alpha_index = _initialize_alpha_index()

    dict_name_list = _get_dict_name_url_list(alphabetical_index=alpha_index, url=False)
    dict_url_list  = _get_dict_name_url_list(alphabetical_index=alpha_index, url=True)

    n_names = sum(len(v) for v in dict_name_list.values())
    n_urls  = sum(len(v) for v in dict_url_list.values())
    print(f"[Résumé] Lettres: {alpha_index} | noms: {n_names} | urls Movie Scripts: {n_urls}", flush=True)

    _build_df_name_url(dict_name_list=dict_name_list, dict_url_list=dict_url_list)

    
    global DICT_ERRORS
    DICT_ERRORS = {"url": [], "error": []}

    # 3) scrap
    _get_all_scripts(dict_url_list=dict_url_list)

    # 4) error file
    _build_error_file()
    print("Terminé ", flush=True)

###--Main execution--
if __name__ == "__main__":
    try:
        main()
    finally:
        try:
            if DRIVER is not None:
                DRIVER.quit()
        except Exception:
            pass
