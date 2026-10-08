"""NYC taxi pipeline: download -> Spark clean -> load Postgres -> dbt build.

The lake upload runs in parallel with the cleaning. Trigger with
{"month": "YYYY-MM"} (default 2025-01). Every task is safe to re-run.
"""
import os
from datetime import datetime, timedelta

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from docker.types import Mount

PROJECT = os.environ["HOST_PROJECT_DIR"]  # project path on the host, for bind mounts
NETWORK = "nyc-taxi-data-platform_default"
MONTH = "{{ dag_run.conf.get('month', '2025-01') }}"
AF_DATA = "/opt/airflow/project/data"

DB_SECRETS = {
    "POSTGRES_USER": os.environ["POSTGRES_USER"],
    "POSTGRES_PASSWORD": os.environ["POSTGRES_PASSWORD"],
    "POSTGRES_DB": os.environ["POSTGRES_DB"],
}
DOCKER = {
    "docker_url": "unix://var/run/docker.sock",
    "network_mode": NETWORK,
    "auto_remove": "success",
    "mount_tmp_dir": False,
}


def bind(sub, target, read_only=False):
    return Mount(source=f"{PROJECT}/{sub}", target=target, type="bind", read_only=read_only)


with DAG(
    dag_id="taxi_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["taxi"],
    doc_md=__doc__,
):
    download_raw = BashOperator(
        task_id="download_raw",
        bash_command=f"python /opt/airflow/project/ingestion/download_tlc.py --zones --month {MONTH}",
        append_env=True,
        env={"DATA_DIR": AF_DATA},
    )

    upload_to_lake = BashOperator(
        task_id="upload_to_lake",
        bash_command="python /opt/airflow/project/ingestion/upload_to_lake.py",
        append_env=True,
        env={"DATA_DIR": AF_DATA, "MINIO_ENDPOINT": "http://minio:9000", "LAKE_BUCKET": "taxi-lake"},
    )

    spark_clean = DockerOperator(
        task_id="spark_clean",
        image="taxi-spark:local",
        command=f"python spark/clean_trips.py --month {MONTH}",
        working_dir="/app",
        environment={"HOME": "/tmp"},
        mounts=[bind("spark", "/app/spark", True), bind("data", "/app/data")],
        mem_limit="4g",
        **DOCKER,
    )

    quality_gate = BashOperator(
        task_id="quality_gate",
        bash_command=(
            "python /opt/airflow/project/quality/check_run.py "
            f"--month {MONTH} "
            "--runs-dir /opt/airflow/project/data/processed/_runs "
            "--out-dir /opt/airflow/project/data/processed/_quality"
        ),
        retries=0,  # bad data will not fix itself, so do not retry
    )

    load_staging = DockerOperator(
        task_id="load_staging",
        image="taxi-loader:local",
        command=f"python warehouse/load_staging.py --month {MONTH}",
        working_dir="/app",
        environment={"POSTGRES_HOST": "postgres", "POSTGRES_PORT": "5432", "DATA_DIR": "/app/data"},
        private_environment=DB_SECRETS,
        mounts=[bind("warehouse", "/app/warehouse", True), bind("data", "/app/data", True)],
        **DOCKER,
    )

    dbt_build = DockerOperator(
        task_id="dbt_build",
        image="taxi-dbt:local",
        command="dbt build",
        working_dir="/dbt",
        environment={"DBT_PROFILES_DIR": "/dbt", "POSTGRES_HOST": "postgres", "POSTGRES_PORT": "5432"},
        private_environment=DB_SECRETS,
        mounts=[bind("dbt", "/dbt")],
        **DOCKER,
    )

    download_raw >> [spark_clean, upload_to_lake]
    spark_clean >> quality_gate >> load_staging >> dbt_build
