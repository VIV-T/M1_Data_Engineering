# Utiliser une image Python récente
FROM python:3.11-slim-bookworm

# Installer Java (requis pour PySpark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copier le fichier requirements.txt
COPY requirements.txt .

# Installer les dépendances Python
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Définir un répertoire de travail
WORKDIR /app
