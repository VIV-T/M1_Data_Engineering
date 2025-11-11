import logging
from pathlib import Path
from typing import List

from pdf2image import convert_from_path
import pytesseract

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Dossier racine du projet = M1_Data_Engineering
PROJECT_ROOT = Path(__file__).resolve().parents[2]
PDF_DIR = PROJECT_ROOT / "pdfs"

def _extract_text_ocr(pdf_path: Path, lang: str = "eng", dpi: int = 300) -> str:
    try:
        logger.info(f"Starting OCR on {pdf_path} (lang={lang}, dpi={dpi})")
        pages = convert_from_path(str(pdf_path), dpi=dpi)
    except Exception as e:
        logger.error(f"PDF to image conversion failed for {pdf_path}: {e}")
        return ""

    texts: List[str] = []
    for i, img in enumerate(pages, start=1):
        try:
            logger.info(f"OCR page {i}/{len(pages)}")
            page_text = pytesseract.image_to_string(img, lang=lang)
            texts.append(page_text)
        except Exception as e:
            logger.error(f"OCR failed on page {i}: {e}")
            continue

    return "\n\n".join(texts)


if __name__ == "__main__":
    #  PDF test in /pdfs/
    pdf_name = "kill-bill-vol-1-2003.pdf"  
    pdf_path = PDF_DIR / pdf_name

    print(f"Using PDF: {pdf_path}")
    text = _extract_text_ocr(pdf_path, lang="eng")

    # Save the results of OCR to a text file
    output_dir = PDF_DIR / "ocr_texts"
    output_dir.mkdir(exist_ok=True)

    output_file = output_dir / f"{pdf_name[:-4]}_ocr.txt"
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(text)

    print(f"\nOCR saved in : {output_file}")
