import os
import csv
import re
import sys
from pathlib import Path

import requests
from pdf2image import convert_from_path
import pytesseract


# Link to file
ROOT = Path(__file__).resolve().parents[2]         # root of project
PDF_DIR = ROOT / "pdfs"                            # pdfs/
DL_DIR = PDF_DIR / "dailyscripts_downloads"        # pdfs/dailyscripts_downloads/
TXT_DIR = PDF_DIR / "ocr_texts"                    # pdfs/ocr_texts/
DL_DIR.mkdir(parents=True, exist_ok=True)
TXT_DIR.mkdir(parents=True, exist_ok=True)

# Default CSV path
DEFAULT_CSV = ROOT / "scrapping_container" / "scrapping" / "imsdb_dailyScripts_comparison_error.csv"

def sanitize(name: str) -> str:
    name = re.sub(r"[^\w\s\-.]", "", name)
    name = re.sub(r"\s+", "_", name.strip())
    return name[:150] or "untitled"

def guess_name(row: dict) -> str:
    for k in ("name", "movie_name", "title"):
        if k in row and row[k] and row[k].strip():
            return row[k]
    url = row.get("movie_url", "") or ""
    tail = url.rstrip("/").split("/")[-1]
    return re.sub(r"\.pdf$", "", tail, flags=re.IGNORECASE) or "script"

def is_pdf_url(url: str) -> bool:
    return isinstance(url, str) and url.lower().endswith(".pdf")

def download_pdf(url: str, dest: Path, timeout: int = 45) -> bool:
    try:
        r = requests.get(url, stream=True, timeout=timeout, headers={"User-Agent":"Mozilla/5.0"})
        if r.status_code != 200:
            print(f"[DOWNLOAD] HTTP {r.status_code} for {url}")
            return False
        with open(dest, "wb") as f:
            for chunk in r.iter_content(128 * 1024):
                if chunk:
                    f.write(chunk)
        return True
    except Exception as e:
        print(f"[DOWNLOAD] error for {url}: {e}")
        return False

def ocr_pdf(pdf_path: Path, lang: str = "eng", dpi: int = 300) -> str:
    try:
        pages = convert_from_path(str(pdf_path), dpi=dpi)
    except Exception as e:
        print(f"[OCR] cannot open {pdf_path.name}: {e}")
        return ""
    texts = []
    for i, img in enumerate(pages, 1):
        try:
            txt = pytesseract.image_to_string(img, lang=lang)
        except Exception as e:
            print(f"[OCR] page {i}/{len(pages)} failed: {e}")
            txt = ""
        texts.append(txt)
    return "\n\n".join(texts)

def ocr_to_txt(pdf_path: Path, txt_path: Path, lang: str = "eng", dpi: int = 300, overwrite: bool = False):
    if txt_path.exists() and not overwrite:
        print(f"[SKIP] {txt_path.name} already exists")
        return
    text = ocr_pdf(pdf_path, lang=lang, dpi=dpi)
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(text)
    print(f"[OK] {txt_path.name}")

def process_csv(csv_path: Path, overwrite_txt: bool = False):
    if not csv_path.exists():
        print(f"[ERROR] CSV not found: {csv_path}")
        return
    print(f"[INFO] Reading: {csv_path}")
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader, 1):
            url = (row.get("movie_url") or "").strip()
            if not is_pdf_url(url):
                continue

            name = sanitize(guess_name(row))
            pdf_file = DL_DIR / f"{name}.pdf"
            txt_file = TXT_DIR / f"{name}_ocr.txt"

            if not pdf_file.exists():
                print(f"[{idx}] Downloading PDF: {url}")
                ok = download_pdf(url, pdf_file)
                if not ok:
                    print(f"[{idx}] -> download failed, skipping")
                    continue
            else:
                print(f"[{idx}] PDF already exists: {pdf_file.name}")

            print(f"[{idx}] OCR -> {txt_file.name}")
            ocr_to_txt(pdf_file, txt_file, lang="eng", dpi=300, overwrite=overwrite_txt)

def main():
    args = sys.argv[1:]
    if not args:
        process_csv(DEFAULT_CSV, overwrite_txt=False)
        return

    if args[0] == "--single":
        if len(args) < 2:
            print("Usage: python ocr.py --single /path/to/file.pdf [--overwrite]")
            return
        pdf = Path(args[1]).resolve()
        overwrite = "--overwrite" in args
        if not pdf.exists():
            print(f"[ERROR] PDF not found: {pdf}")
            return
        name = sanitize(pdf.stem)
        txt_file = TXT_DIR / f"{name}_ocr.txt"
        print(f"[SINGLE] OCR {pdf.name} -> {txt_file.name}")
        ocr_to_txt(pdf, txt_file, lang="eng", dpi=300, overwrite=overwrite)
        return

    csv_path = Path(args[0]).resolve()
    overwrite = "--overwrite" in args
    process_csv(csv_path, overwrite_txt=overwrite)

if __name__ == "__main__":
    main()
