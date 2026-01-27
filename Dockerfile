# Dockerfile for TMDB Content Analytics Pipeline
FROM apache/airflow:2.8.1-python3.11

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements and install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy project files
COPY --chown=airflow:root ./dags /opt/airflow/dags
COPY --chown=airflow:root ./scripts /opt/airflow/scripts
COPY --chown=airflow:root ./queries /opt/airflow/queries
COPY --chown=airflow:root ./schema /opt/airflow/schema
COPY --chown=airflow:root ./config.yaml /opt/airflow/config.yaml

# Set Python path to include scripts directory
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/scripts"

# Set working directory
WORKDIR /opt/airflow
