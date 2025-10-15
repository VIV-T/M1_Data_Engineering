#!/usr/bin/env python3
"""
Minimal runner for the scrapper image. This script DOES NOT import Airflow
and can be used as the container entrypoint when launched by DockerOperator.
"""
import logging
import requests
import os

def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Scrapper runner started")
    logging.info("Current working directory: %s", os.getcwd())

    # Simple health check to ensure network / python works in the image.
    try:
        r = requests.get('https://example.com', timeout=5)
        logging.info("Fetched example.com: status=%s", r.status_code)
    except Exception as exc:
        logging.exception("Network check failed: %s", exc)

    try :
        # Write output to the shared mount path used by the DAG (inside the
        # container this will be /scrapping because we mount the 'scrapper-data'
        # volume there). Using an absolute path avoids ambiguity in working dir.
        out_path = os.environ.get('SCRAPPER_OUTPUT_PATH', '/scrapping/data_scripts/run_scraper.txt')
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with open(out_path, "a") as f :
            f.write(f"run_scraper executed\nCurrent working dir : {os.getcwd()}\n")
            #{os.listdir("/opt/airflow")}
    except Exception as exc:
         logging.exception("File write failed: %s", exc)

    logging.info("Scrapper runner finished")


if __name__ == '__main__':
    main()
