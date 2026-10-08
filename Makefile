.RECIPEPREFIX := >
.DEFAULT_GOAL := help
MONTH ?= 2025-01

help: ## Show this list
> @grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | sed 's/:.*## / - /'

bootstrap: ## Create .env with random secrets (never overwrites values you set)
> ./scripts/bootstrap.sh

core: ## Start MinIO and Postgres
> docker compose up -d minio postgres

airflow: core ## Start Airflow, then open http://localhost:8080
> docker compose --profile airflow up -d airflow-apiserver airflow-scheduler airflow-dag-processor

stop-airflow: ## Stop Airflow only (saves about 1 GB of memory)
> docker compose --profile airflow stop

pipeline: ## Run the pipeline for one month: make pipeline MONTH=2025-01
> docker compose --profile airflow exec airflow-scheduler airflow dags unpause taxi_pipeline
> docker compose --profile airflow exec airflow-scheduler airflow dags trigger taxi_pipeline --conf '{"month": "$(MONTH)"}'

dbt: ## Build and test the dbt models
> docker compose run --rm dbt dbt build

test: ## Run all unit tests (host, Spark container, loader container)
> pytest -q
> docker compose run --rm spark python -m pytest tests/spark -q -p no:cacheprovider
> docker compose run --rm loader python -m pytest tests/warehouse -q -p no:cacheprovider

lint: ## Check code style
> ruff check .

ps: ## Show running containers
> docker compose --profile airflow ps

down: ## Stop everything (your data is kept)
> docker compose --profile airflow --profile spark --profile tools stop
> docker compose stop

.PHONY: help bootstrap core airflow stop-airflow pipeline dbt test lint ps down
