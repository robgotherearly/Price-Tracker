from __future__ import annotations

import json
from datetime import datetime

from docker.types import Mount

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.docker.operators.docker import DockerOperator

WAREHOUSE_CONN_ID = "warehouse_postgres"

RAW_TABLE_DDL = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.prices_raw (
  load_ts        TIMESTAMP NOT NULL DEFAULT NOW(),
  payload        JSONB NOT NULL
);
"""


def ensure_conn_and_tables():
    hook = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID)
    hook.run(RAW_TABLE_DDL)


def ingest_to_raw():
    # Import inside task to avoid Airflow parse-time issues
    import sys
    sys.path.append("/opt/airflow/ingestion")

    from api_ingest import fetch_prices

    rows = fetch_prices()
    hook = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID)

    values = [(json.dumps(r.model_dump()),) for r in rows]
    hook.insert_rows(
        table="raw.prices_raw",
        rows=values,
        target_fields=["payload"],
        commit_every=1000,
        replace=False,
    )


default_args = {"owner": "data-eng", "retries": 1}

with DAG(
    dag_id="retail_price_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["portfolio", "dbt", "postgres"],
) as dag:

    t_ensure = PythonOperator(
        task_id="ensure_raw_schema",
        python_callable=ensure_conn_and_tables,
    )

    t_ingest = PythonOperator(
        task_id="ingest_api_to_raw",
        python_callable=ingest_to_raw,
    )

    # -------------------------------
    # dbt run/test inside dbt container (Mounts, not volumes)
    # -------------------------------
    docker_sock_mount = Mount(
        source="/var/run/docker.sock",
        target="/var/run/docker.sock",
        type="bind",
    )

    dbt_project_mount = Mount(
        source="/opt/airflow/dbt",  # dbt project path inside Airflow container
        target="/usr/app",
        type="bind",
    )

    t_dbt_run = DockerOperator(
        task_id="dbt_run",
        image="ghcr.io/dbt-labs/dbt-postgres:1.8.2",
        api_version="auto",
        auto_remove=True,
        command="dbt run --profiles-dir /usr/app --project-dir /usr/app",
        docker_url="unix://var/run/docker.sock",
        network_mode="retail-price-tracker_default",
        working_dir="/usr/app",
        environment={"DBT_PROFILES_DIR": "/usr/app"},
        mounts=[docker_sock_mount, dbt_project_mount],
    )

    t_dbt_test = DockerOperator(
        task_id="dbt_test",
        image="ghcr.io/dbt-labs/dbt-postgres:1.8.2",
        api_version="auto",
        auto_remove=True,
        command="dbt test --profiles-dir /usr/app --project-dir /usr/app",
        docker_url="unix://var/run/docker.sock",
        network_mode="retail-price-tracker_default",
        working_dir="/usr/app",
        environment={"DBT_PROFILES_DIR": "/usr/app"},
        mounts=[docker_sock_mount, dbt_project_mount],
    )

    # dependency chain
    t_ensure >> t_ingest >> t_dbt_run >> t_dbt_test
