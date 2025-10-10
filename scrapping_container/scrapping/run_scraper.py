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

    logging.info("Scrapper runner finished")


if __name__ == '__main__':
    main()
