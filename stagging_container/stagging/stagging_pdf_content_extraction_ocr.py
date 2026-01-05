### --Imports--
import logging
import os
import glob

from pdf2image import convert_from_path
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
        del img # remove the object form the RAM to avoid error
    return "\n\n".join(texts)


# function to build the new filepath based on the file_path.
def build_file_path(file_path : str):
    filename = os.path.splitext(os.path.basename(file_path))[0]
    new_filename = filename + ".txt"
    new_file_path = os.path.join(STAGGING_FOLDER, "scripts", new_filename)
    logger.info(f"New file path created : {new_file_path}")
    return new_file_path



### --Main function--
def main_stagging_ocr():
    # get the filename list (pdf files in the volume). => use glob
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
        pages = convert_pdf_to_img_list(pdf_path=pdf_path)
        script_txt = extract_txt_with_ocr(pages=pages)
        
        del pages # remove the object form the RAM to avoid error

        with open(file=new_file_path, mode="w", encoding="utf-8") as f :
            f.write(script_txt)
    
    logger.info("Text extraction finished.")
