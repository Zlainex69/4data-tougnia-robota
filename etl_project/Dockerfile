
FROM python:3.11-slim

# dépendances nécessaires
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# répertoire de travail dans le conteneur
WORKDIR /app

# Cp les fichiers de dépendances
COPY pyproject.toml setup.cfg setup.py ./
COPY etl_project ./etl_project
COPY etl_project_tests ./etl_project_tests
COPY tmp2xky_gzc ./tmp2xky_gzc

# Installation de Dagster et les dépendances
RUN pip install --upgrade pip
RUN pip install .
RUN pip install dagster dagster-webserver duckdb pandas requests folium matplotlib


# Exposer le port pour Dagster UI
EXPOSE 3000

# Commande par défaut
CMD ["dagster", "dev", "-f", "etl_project/pipeline.py"]
