FROM python:3.11

WORKDIR /app/scrapping
COPY requirements.txt ./
COPY scrapping/ .

# Create virtualenv outside the application directory so a bind-mount doesn't hide it
RUN python -m venv /opt/venv_scrapping
RUN /opt/venv_scrapping/bin/pip install --upgrade pip
RUN /opt/venv_scrapping/bin/pip install -r requirements.txt

# Chromium installation - required for Selenium
USER root
RUN apt-get update && \
    apt-get install -y chromium chromium-driver && \
    rm -rf /var/lib/apt/lists/*

    
# Default command to run the scrapping DAG using the venv in /opt
#CMD ["/opt/venv_scrapping/bin/python", "/app/scrapping/scrapping_dag.py"]

COPY scrapping/main.py /app/scrapping/main.py

CMD ["/opt/venv_scrapping/bin/python", "/app/scrapping/main.py"]