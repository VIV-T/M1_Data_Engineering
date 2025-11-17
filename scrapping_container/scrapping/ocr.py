### Imports
import pandas as pd
import os
import re
import requests
from pathlib import Path

from pdf2image import convert_from_path
import pytesseract


### Initialization
# Download CSV
csv_path = 'scrapping_container/scrapping/data_scrapping/statistics/df_final_scrapping.csv'
df = pd.read_csv(csv_path)

# Creation od directories
DL_DIR = Path('pdfs/downloads')
TXT_DIR = Path('pdfs/ocr_texts')
DL_DIR.mkdir(parents=True, exist_ok=True)
TXT_DIR.mkdir(parents=True, exist_ok=True)


### Tools
# Clean file name
def sanitize(name):
    name = re.sub(r'[^\w\s.-]', '', name)
    name = re.sub(r'\s+', '_', name.strip())
    return name[:150] or 'untitled'


# Download (locally) PDF based on the URL store in the csv file
def download_pdf(url, destination):
    try:
        r = requests.get(url, stream=True, timeout=45, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200:
            print(f"[DOWNLOAD] HTTP {r.status_code} for {url}")
            return False
        with open(destination, "wb") as f:
            for chunk in r.iter_content(128 * 1024):
                if chunk:
                    f.write(chunk)
        return True
    except Exception as e:
        print(f"[DOWNLOAD] Error for {url}: {e}")
        return False


# Function to apply the OCR on pdf files. Output is a string containing the extracted text.
# Nt : dpi = dots per inch (quality of the image conversion)
def ocr_pdf(pdf_path, lang="eng", dpi=300):
    try:
        pages = convert_from_path(str(pdf_path), dpi=dpi)
    except Exception as e:
        print(f"[OCR] Can't open {pdf_path.name}: {e}")
        return ""
    texts = []
    for i, img in enumerate(pages, 1):
        # apply OCR on each page (img)
        try:
            txt = pytesseract.image_to_string(img, lang=lang)
        except Exception as e:
            print(f"[OCR] Failure of pages {i}/{len(pages)}: {e}")
            txt = ""
        texts.append(txt)
    return "\n\n".join(texts)


### Main loop
# Go through entire CSV
for index, row in df[df['scrapping_url_extension'] == 'pdf'].iterrows():
    pdf_url = row['scrapping_url']
    movie_name = row['movie_name_conventioned']

    # Clean film name
    sanitized_name = sanitize(movie_name)

    # Links to PDF and text 
    pdf_file = DL_DIR / f"{sanitized_name}.pdf"
    txt_file = TXT_DIR / f"{sanitized_name}_ocr.txt"

    # Download PDF
    if not pdf_file.exists():
        print(f"[{index}] Download of PDF: {pdf_url}")
        ok = download_pdf(pdf_url, pdf_file)
        if not ok:
            print(f"[{index}] -> Failed to download")
            continue
    else:
        print(f"[{index}] PDF already exists: {pdf_file.name}")

    # OCR
    print(f"[{index}] OCR -> {txt_file.name}")
    ocr_text = ocr_pdf(pdf_file, lang="eng", dpi=300)

    # Save text file from OCR
    with open(txt_file, "w", encoding="utf-8") as f:
        f.write(ocr_text)

    print(f"[{index}] OCR done and saved in: {txt_file.name}")

print("OCR done for every PDF")
