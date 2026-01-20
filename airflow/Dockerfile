FROM apache/airflow:2.9.3

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.12
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

COPY requirements.txt /requirements.txt

# Install Airflow providers using constraints (safe)
RUN pip install --no-cache-dir -c ${CONSTRAINT_URL} \
      apache-airflow-providers-postgres \
      apache-airflow-providers-docker

# Install your extra libs WITHOUT constraints (avoid pydantic conflict)
RUN pip install --no-cache-dir -r /requirements.txt
