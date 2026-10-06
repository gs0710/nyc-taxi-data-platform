FROM python:3.12-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends git \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir dbt-core==1.12.5 dbt-postgres==1.11.0

RUN useradd -m -u 1000 dbt
USER dbt
WORKDIR /dbt
