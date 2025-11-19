FROM python:3.11

# to be located inside the /app folder
WORKDIR /app

# copy the associated requirements.txt file => to install mandatory python package later.
COPY requirements.txt ./stagging_container/requirements_stagging.txt

# copy the content of the local folder inside a Docker Bind => allow us to access to those files and folders inside the container. 
# very important !
COPY /stagging ./stagging_container/stagging_files

# Create virtualenv outside the application directory so a bind-mount doesn't hide it
RUN python -m venv /opt/venv_stagging
RUN /opt/venv_stagging/bin/pip install --upgrade pip
RUN /opt/venv_stagging/bin/pip install -r ./stagging_container/requirements_stagging.txt

