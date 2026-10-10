# NYC Taxi Data Platform

[![CI](https://github.com/gs0710/nyc-taxi-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/gs0710/nyc-taxi-data-platform/actions/workflows/ci.yml)

An end-to-end batch data engineering project: NYC TLC taxi trips plus Open-Meteo
weather, processed with PySpark, modeled with dbt into a Postgres star schema,
orchestrated by Airflow, and visualized in Metabase.

**Status:** In progress (Phase 1: project setup)

## Planned stack
Python, PySpark, PostgreSQL, dbt, Airflow, Docker, MinIO, GitHub Actions, Metabase

## Setup
1. Clone the repo
2. `cp .env.example .env` and fill in your own values
3. `python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt`
