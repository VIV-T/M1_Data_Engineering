# syntax=docker/dockerfile:1.4
# Utiliser une image Python récente
FROM python:3.11-slim-bookworm

# Installer Java (requis pour PySpark) et support venv
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless python3-venv ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copier le fichier requirements.txt en amont pour tirer parti du cache Docker
COPY requirements.txt .

# Créer le virtualenv
RUN python -m venv /opt/venv_analysis

# Installer les dépendances Python en utilisant un cache pour pip (BuildKit cache mount)
# Requires BuildKit: build with `DOCKER_BUILDKIT=1 docker build ...`
RUN --mount=type=cache,target=/root/.cache/pip /opt/venv_analysis/bin/python -m pip install --upgrade pip setuptools wheel && \
    /opt/venv_analysis/bin/pip install -r requirements.txt

# Copier le reste du contexte après l'installation pour ne pas invalider la couche pip
COPY . ./analysis_container

# Définir un répertoire de travail
WORKDIR /app

# Exposer le venv dans le PATH (optionnel)
ENV PATH="/opt/venv_analysis/bin:${PATH}"
