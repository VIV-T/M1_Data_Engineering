### --Imports--
import logging

from pdf2image import convert_from_path
import pytesseract

# logger (based on the "main.py" script config)
logger = logging.getLogger("stagging_pdf_content_extraction_ocr")


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
    return "\n\n".join(texts)



### --Main function--
def main_stagging_ocr():
    # get the filename list (pdf files in the volume). => use glob
    # for each filename :
    #   - build the file path (based on global variables related to the volume)
    #   - apply the two functions : "convert_pdf_to_img_list" and "extract_txt_with_ocr"
    #   - write the results into a new file inside the volume (stagging folder)

    # Nt : There are multiple path and variable to consider when interacting with the volume :
    #           -   the input folder : scrapping_data folder 
    #           -   the output folder : stagging_data folder 

    pass