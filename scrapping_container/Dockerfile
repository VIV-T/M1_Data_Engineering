FROM python:3.11

WORKDIR /app/scrapping

# Copie des fichiers du scrapping
COPY scrapping/ .

# Création et activation de l'environnement virtuel
RUN python -m venv venv_scrapping
RUN . venv_scrapping/bin/activate && pip install --upgrade pip
RUN . venv_scrapping/bin/activate && pip install -r requirements.txt

# Chromium installation - required for Selenium
USER root
RUN apt-get update && \
    apt-get install -y chromium chromium-driver && \
    rm -rf /var/lib/apt/lists/*

    
# Commande par défaut pour exécuter le DAG de scrapping
CMD ["./venv_scrapping/bin/python", "scrapping_dag.py"]