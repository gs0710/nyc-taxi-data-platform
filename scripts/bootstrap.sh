#!/usr/bin/env bash
# Creates .env from .env.example and fills every secret with a random value.
# Safe to re-run: values that are already set are never changed.
# Set ENV_FILE=/path to write somewhere other than ./.env (used for testing).
set -euo pipefail

cd "$(dirname "$0")/.."
ENV_FILE="${ENV_FILE:-.env}"

[ -f .env.example ] || { echo "ERROR: .env.example not found" >&2; exit 1; }
command -v openssl >/dev/null || { echo "ERROR: openssl is required" >&2; exit 1; }

[ -f "$ENV_FILE" ] || cp .env.example "$ENV_FILE"
chmod 600 "$ENV_FILE"

get() { grep -E "^$1=" "$ENV_FILE" | tail -n1 | cut -d= -f2- || true; }

set_value() {  # set_value KEY VALUE: replace the line if it exists, else append it
  local key="$1" value="$2" tmp
  tmp="$(mktemp)"
  grep -v -E "^${key}=" "$ENV_FILE" > "$tmp" || true
  printf '%s=%s\n' "$key" "$value" >> "$tmp"
  cat "$tmp" > "$ENV_FILE"
  rm -f "$tmp"
}

fill_if_unset() {  # fill_if_unset KEY VALUE
  local current
  current="$(get "$1")"
  if [ -z "$current" ] || [[ "$current" == change_me* ]]; then
    set_value "$1" "$2"
    echo "set $1"
  fi
}

guard_volume() {  # guard_volume KEY VOLUME: refuse to replace a password that a data volume already uses
  local current
  current="$(get "$1")"
  [ -z "$current" ] || [[ "$current" == change_me* ]] || return 0
  command -v docker >/dev/null 2>&1 || return 0
  docker volume inspect "$2" >/dev/null 2>&1 || return 0
  [ "${FORCE:-0}" = "1" ] && return 0
  echo "ERROR: $1 is not set in $ENV_FILE, but the Docker volume '$2' already exists." >&2
  echo "       A new random password would not match the one stored in that volume." >&2
  echo "       Put the original value in $ENV_FILE, or run FORCE=1 ./scripts/bootstrap.sh to reset" >&2
  echo "       it on purpose (see docs/docker.md)." >&2
  exit 1
}

guard_volume POSTGRES_PASSWORD nyc-taxi-data-platform_pg_data
guard_volume AIRFLOW_DB_PASSWORD nyc-taxi-data-platform_pg_data
guard_volume MINIO_ROOT_PASSWORD nyc-taxi-data-platform_minio_data

for key in POSTGRES_PASSWORD MINIO_ROOT_PASSWORD AIRFLOW_DB_PASSWORD AIRFLOW_ADMIN_PASSWORD; do
  fill_if_unset "$key" "$(openssl rand -hex 16)"
done
fill_if_unset AIRFLOW_JWT_SECRET "$(openssl rand -hex 32)"
fill_if_unset AIRFLOW_FERNET_KEY "$(openssl rand -base64 32 | tr '+/' '-_')"
fill_if_unset AIRFLOW_UID "$(id -u)"
fill_if_unset HOST_PROJECT_DIR "$PWD"

docker_gid="$(getent group docker | cut -d: -f3 || true)"
if [ -n "$docker_gid" ]; then
  fill_if_unset DOCKER_GID "$docker_gid"
else
  echo "WARNING: no 'docker' group found, DOCKER_GID not set (Airflow cannot start containers)" >&2
fi

echo "Done. Review $ENV_FILE and keep it private (it is git-ignored)."
