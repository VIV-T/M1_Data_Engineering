# Image de base
FROM ubuntu:22.04

# Évite les prompts interactifs
ENV DEBIAN_FRONTEND=noninteractive

# Installation des dépendances système
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    cmake \
    build-essential \
    wget \
    curl \
    ca-certificates \
    python3 \
    python3-pip \
    python3-venv \
    libopenblas-dev \
    libcurl4-openssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Téléchargement de llama.cpp (sans git clone)
WORKDIR /llama.cpp
RUN curl -L https://github.com/ggerganov/llama.cpp/archive/refs/heads/master.tar.gz \
    | tar -xz --strip-components=1

# Compilation (stable en environnement Docker)
RUN mkdir -p build && \
    cd build && \
    cmake \
      -DLLAMA_BLAS=OFF \
      -DLLAMA_CURL=ON \
      -DCMAKE_BUILD_TYPE=Release \
      .. && \
    make -j1

# Application folder
WORKDIR /app
COPY requirements.txt ./preparation_container/requirements.txt
COPY /app ./preparation_container/app

# Creation of the virtual environment & installation of python packages
RUN python3 -m venv /opt/venv_preparer && \
    if [ ! -e /opt/venv_preparer/bin/python3 ]; then \
        ln -s /opt/venv_preparer/bin/python /opt/venv_preparer/bin/python3; \
    fi
RUN /opt/venv_preparer/bin/pip install --upgrade pip
RUN /opt/venv_preparer/bin/pip install -r ./preparation_container/requirements.txt

# NOTE: Models should be stored and managed by the `ollama` service.
# Removed model download here to avoid duplicating models in the preparation image.