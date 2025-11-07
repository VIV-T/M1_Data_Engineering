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

#  Config 
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
            logging.warning(
                f"[{attempt}/{RETRY_COUNT}] GET failed {url}: {e} | retry in {wait:.1f}s"
            )
            time.sleep(wait)
    raise last_err  # type: ignore

def _fetch_soup(url: str) -> BeautifulSoup:
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

#  Alphabet index 
def _initialize_alpha_index() -> List[str]:
    alpha = ["0"] + [chr(i) for i in range(65, 91)]
    logging.info(f"Alphabet index: {alpha}")
    return alpha

#  Index listing 
def _get_name_url_list(letter: str, url: bool = False):
    """
    If url=False -> list movie names (for info only).
    If url=True  -> list 'Movie Scripts/XXX Script.html' URLs.
    """
    target = f"{BASE}/alphabetical/{letter}"
    urls, names = [], []

    # Selenium first
    if DRIVER is not None:
        try:
            DRIVER.get(target)
            time.sleep(1.0)
            elems = DRIVER.find_elements(By.XPATH, "//*[@id='mainbody']/table[2]//td[3]//a")
            if not elems:
                elems = DRIVER.find_elements(
                    By.XPATH, "//*[@id='mainbody']/table[2]/tbody/tr/td[3]//a"
                )
            for e in elems:
                txt = (e.text or "").strip()
                href = (e.get_attribute("href") or "").strip()
                if txt:
                    names.append(txt)
                if "/Movie%20Scripts/" in href and href.endswith(".html"):
                    urls.append(href)
        except Exception as e:
            logging.warning(f"[{letter}] Selenium index failed: {e}")

    # Fallback: requests+BS4
    if not urls:
        soup = _fetch_soup(target)
        for a in soup.find_all("a", href=True):
            href = a["href"]
            txt = a.get_text(strip=True)
            if txt:
                names.append(txt)
            if "/Movie Scripts/" in href and href.endswith(".html"):
                urls.append(urljoin(BASE, href.replace(" ", "%20")))

    # Dedup
    def _dedup(seq):
        seen, out = set(), []
        for x in seq:
            if x not in seen:
                seen.add(x)
                out.append(x)
        return out

    names = _dedup(names)
    urls = _dedup(urls)

    return urls if url else names

def _get_dict_url_list(alpha_index: List[str]):
    d = {}
    for letter in alpha_index:
        d[letter] = _get_name_url_list(letter, url=True)
        logging.info(f"[{letter}] {len(d[letter])} Movie Scripts URLs")
    return d

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

#  Metadata : title,genre and user ratings
def _extract_title(soup: BeautifulSoup) -> str:
    h1 = soup.find("h1")
    if h1:
        return _norm(h1.get_text())
    title = soup.find("title")
    return _norm(title.get_text()) if title else ""

def _extract_user_rating(soup: BeautifulSoup) -> Optional[float]:
    label = soup.find(
        lambda tag: tag.name in ("b", "strong")
        and tag.get_text(strip=True).lower() == "average user rating"
    )
    if not label:
        full_txt = _norm(soup.get_text(" "))
        m = re.search(
            r"Average user rating[^0-9]*(\d+(?:\.\d+)?)\s*out of\s*10",
            full_txt, flags=re.I
        )
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

    # local "(X out of 10)"
    for node in label.next_elements:
        if isinstance(node, NavigableString):
            m = re.search(r"(\d+(?:\.\d+)?)\s*out of\s*10", str(node), flags=re.I)
            if m:
                try:
                    return float(m.group(1))
                except ValueError:
                    pass
        elif isinstance(node, Tag) and node.name in ("b", "strong"):
            break

    # local stars image
    for node in label.next_elements:
        if isinstance(node, Tag) and node.name == "img":
            m = re.search(r"/images/rating/(\d+)-stars\.gif$", node.get("src", ""))
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
    gtag = soup.find(
        lambda tag: tag.name in ("b", "strong")
        and tag.get_text(strip=True) == "Genres"
    )
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
    seen, out = set(), []
    for g in genres:
        if g and g not in seen:
            seen.add(g)
            out.append(g)
    return out

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
    alpha_index = _initialize_alpha_index()
    dict_url_list = _get_dict_url_list(alpha_index)
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
