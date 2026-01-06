# syntax=docker/dockerfile:1.4
# Use a recent python image as base
FROM python:3.11-slim-bookworm

# Install Java (required for PySpark) and venv support
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless python3-venv ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy the requirements.txt file upstream to take advantage of the Docker cache.
COPY requirements.txt .

# Create the venv 
RUN python -m venv /opt/venv_analysis

# Install Python dependencies using a cache for pip (BuildKit cache mount)
# Requires BuildKit: build with `DOCKER_BUILDKIT=1 docker build ...`
RUN --mount=type=cache,target=/root/.cache/pip /opt/venv_analysis/bin/python -m pip install --upgrade pip setuptools wheel && \
    /opt/venv_analysis/bin/pip install -r requirements.txt

# Copy the rest of the context after installation so as not to invalidate the pip layer.
COPY . /app/analysis_container

# Define the workdir
WORKDIR /app

# Expose the venv path (optional)
ENV PATH="/opt/venv_analysis/bin:${PATH}"
