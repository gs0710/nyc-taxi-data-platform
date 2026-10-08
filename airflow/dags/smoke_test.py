"""Smoke test: proves Airflow runs a task and can start a Docker container."""
from datetime import datetime

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    dag_id="smoke_test",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test"],
):
    hello = BashOperator(
        task_id="hello",
        bash_command="echo 'hello from airflow' && ls /opt/airflow/project",
    )

    docker_check = DockerOperator(
        task_id="docker_check",
        image="taxi-loader:local",
        command=["python", "-c", "print('hello from a container started by Airflow')"],
        docker_url="unix://var/run/docker.sock",
        network_mode="nyc-taxi-data-platform_default",
        auto_remove="success",
        mount_tmp_dir=False,
    )

    hello >> docker_check
