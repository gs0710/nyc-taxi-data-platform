# Docker setup

Everything runs in Docker Compose. Only Python, Git, Docker and `make` are needed on the host.

## Quick start

```bash
make bootstrap                 # creates .env with random secrets (never overwrites values)
make build                     # builds the Spark, loader, dbt and Airflow images
make core                      # starts MinIO and Postgres
make airflow                   # starts Airflow, UI at http://localhost:8080
make pipeline MONTH=2025-01    # runs download -> Spark -> quality gate -> load -> dbt
```

The Airflow login is `airflow` and the password is `AIRFLOW_ADMIN_PASSWORD` in your `.env`.

## Services

| Service | Image | Profile | Port | Purpose |
|---|---|---|---|---|
| `minio` | `pgsty/minio` (community fork) | default | 9000 API, 9001 console | S3-compatible data lake |
| `postgres` | `postgres:17-alpine` | default | 127.0.0.1:5432 | Warehouse (`taxi_dw`) and Airflow metadata (`airflow`) |
| `spark` | `taxi-spark:local` | `spark` | none | PySpark cleaning job, 4 GB limit, started by Airflow |
| `loader` | `taxi-loader:local` | `tools` | none | COPY-based load into Postgres staging |
| `dbt` | `taxi-dbt:local` | `tools` | none | dbt build and tests |
| `airflow-init` | `taxi-airflow:local` | `airflow` | none | One-time DB migration and admin user |
| `airflow-apiserver` | `taxi-airflow:local` | `airflow` | 127.0.0.1:8080 | Airflow web UI and API |
| `airflow-scheduler` | `taxi-airflow:local` | `airflow` | none | Schedules and runs tasks (LocalExecutor) |
| `airflow-dag-processor` | `taxi-airflow:local` | `airflow` | none | Parses DAG files |

Services with a profile do not start with a plain `docker compose up`, which saves memory.
Measured at idle during development: Postgres about 130 MiB, Airflow scheduler about 450 MiB,
API server about 270 MiB, DAG processor about 190 MiB. Check your own numbers with
`docker stats --no-stream` and `docker system df`.

## Where data lives

- Named volumes `pg_data` (Postgres) and `minio_data` (lake). `make down` keeps them.
- `./data` on the host holds raw files and Spark output. It is git-ignored.
- Never run `docker compose down -v` on the real project unless you want to delete the data.

## Known limitations

- **Docker socket:** Airflow mounts `/var/run/docker.sock` so tasks can start containers.
  That gives Airflow root-equivalent control of Docker. Acceptable for a local project,
  not for production (use Kubernetes or a cloud service there).
- **UID 1000:** the Spark, loader and dbt images run as user 1000, the default first user
  on Ubuntu and WSL. Other UIDs may hit permission errors on `./data`.
- **MinIO ports** are published on all interfaces. Postgres and Airflow use 127.0.0.1 only.
- **Spark reads raw files from local disk**, not from the lake. The lake upload keeps a copy.
- **Fixed container names** (`taxi-postgres`, `taxi-minio`) mean only one copy of the stack can run.
- **MinIO:** upstream stopped publishing free images, so a community fork is used.
- The Airflow setup is a learning setup (LocalExecutor, no workers), not a production one.
