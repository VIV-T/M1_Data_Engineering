### --Imports--
import logging
import os
import glob
import gc

from pdf2image import convert_from_path, pdfinfo_from_path
import pytesseract

# logger (based on the "main.py" script config)
logger = logging.getLogger("stagging_pdf_content_extraction_ocr")


INGESTION_FOLDER = "./project_data/ingestion_data"
PDF_FOLDER = os.path.join(INGESTION_FOLDER, "scripts", "pdf_data")
STAGGING_FOLDER = "./project_data/stagging_data"

### --Tools--
# Converted the pdf pages to images : mandatory to use the OCR to extract the pdf content.
# Nt : dpi = dots per inch (quality of the image conversion)
def convert_pdf_to_img_list(pdf_path : str, dpi = 300) :
    try:
        logger.info(f"Starting convertion to img - {pdf_path}")
        pages = convert_from_path(str(pdf_path), dpi=dpi)
        logger.info(f"convert_pdf_to_img_list - {pdf_path} succefully converted to an image list")
        return pages
    except Exception as e:
        logger.error(f"convert_pdf_to_img_list - Can't open {pdf_path} : {e}")
        return []
        

# Use the OCR : pytesseract to extract the text from the pages (converted to images) of the pdf files.
def extract_txt_with_ocr(pages : list, lang="eng"):
    texts = []
    for i, img in enumerate(pages, 1):
        # apply OCR on each page (img)
        try:
            txt = pytesseract.image_to_string(img, lang=lang)
        except Exception as e:
            logger.error(f"ocr_pdf - Failure of pages {i}/{len(pages)}: {e}")
            txt = ""
        texts.append(txt)
        try:
            img.close()
        except Exception:
            pass
        del img # remove the object form the RAM to avoid error
    return "\n\n".join(texts)


def ocr_and_write_pdf_stream(pdf_path: str, new_file_path: str, dpi: int = 300, lang: str = "eng"):
    """Process a PDF one page at a time and append OCR output to `new_file_path`.
    This avoids loading all pages into memory for large PDFs.
    """
    try:
        info = pdfinfo_from_path(str(pdf_path))
        total_pages = int(info.get("Pages", 0))
        logging.info(f"{pdf_path} contains {total_pages} pages")
    except Exception as e:
        logger.error(f"ocr_and_write_pdf_stream - can't read pdf info for {pdf_path}: {e}")
        total_pages = 0

    # Ensure output dir exists
    os.makedirs(os.path.dirname(new_file_path), exist_ok=True)

    with open(new_file_path, mode="w", encoding="utf-8") as f:
        for p in range(1, total_pages + 1):
            try:
                imgs = convert_from_path(str(pdf_path), dpi=dpi, first_page=p, last_page=p)
                if not imgs:
                    logger.warning(f"No image returned for page {p} of {pdf_path}")
                    continue
                img = imgs[0]
                try:
                    txt = pytesseract.image_to_string(img, lang=lang)
                except Exception as e:
                    logger.error(f"ocr_pdf - Failure of page {p}/{total_pages} for {pdf_path}: {e}")
                    txt = ""
                f.write(txt)
                f.write("\n\n")
                f.flush()
                try:
                    img.close()
                except Exception:
                    pass
                del img
                del imgs
                gc.collect()
            except Exception as e:
                logger.error(f"ocr_and_write_pdf_stream - error processing page {p} of {pdf_path}: {e}")
                continue
            logger.info(f"{p} pages over {total_pages} processed.")


# function to build the new filepath based on the file_path.
def build_file_path(file_path : str):
    filename = os.path.splitext(os.path.basename(file_path))[0]
    new_filename = filename + ".txt"
    new_file_path = os.path.join(STAGGING_FOLDER, "scripts", new_filename)
    logger.info(f"New file path created : {new_file_path}")
    return new_file_path



### --Main function--
def main_stagging_ocr():
    # for each filename :
    #   - build the file path (based on global variables related to the volume)
    #   - apply the two functions : "convert_pdf_to_img_list" and "extract_txt_with_ocr"
    #   - write the results into a new file inside the volume (stagging folder)

    # Nt : There are multiple path and variable to consider when interacting with the volume :
    #           -   the input folder : ingestion_data folder 
    #           -   the output folder : stagging_data folder 

    logger.info("Starting the text extraction from pdf files.")
    # Nt glob allow us to iter on all the file inside a folder.
    for pdf_path in glob.glob(f"{PDF_FOLDER}/*.pdf"):
        logger.info(f"{pdf_path} start the process")
        new_file_path = build_file_path(file_path=pdf_path)
        # Use streaming per-page processing to avoid high memory usage
        ocr_and_write_pdf_stream(pdf_path=pdf_path, new_file_path=new_file_path)
    
    logger.info("Text extraction finished.")
