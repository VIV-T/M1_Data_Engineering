FROM python:3.11

# to be located inside the /app folder
WORKDIR /app

# copy the associated requirements.txt file => to install mandatory python package later.
COPY requirements.txt ./scrapping_container/requirements_scrapper.txt

# copy the content of the local folder inside a Docker Bind => allow us to access to those files and folders inside the container. 
# very important !
COPY /scrapping ./scrapping_container/scrapping_files

# Create virtualenv outside the application directory so a bind-mount doesn't hide it
RUN python -m venv /opt/venv_scrapping
RUN /opt/venv_scrapping/bin/pip install --upgrade pip
RUN /opt/venv_scrapping/bin/pip install -r ./scrapping_container/requirements_scrapper.txt

# Chromium installation - required for Selenium
USER root
RUN apt-get update && \
    apt-get install -y chromium chromium-driver && \
    rm -rf /var/lib/apt/lists/*
