#!/usr/bin/env bash
set -euo pipefail

POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-pgqrs-buildkite-postgres}"
PGBOUNCER_CONTAINER="${PGBOUNCER_CONTAINER:-pgqrs-buildkite-pgbouncer}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
PGBOUNCER_PORT="${PGBOUNCER_PORT:-6432}"

start_postgres() {
  docker rm -f "${POSTGRES_CONTAINER}" >/dev/null 2>&1 || true
  docker run -d \
    --name "${POSTGRES_CONTAINER}" \
    -p "${POSTGRES_PORT}:5432" \
    -e POSTGRES_PASSWORD=postgres \
    -e POSTGRES_USER=postgres \
    -e POSTGRES_DB=postgres \
    -e POSTGRES_HOST_AUTH_METHOD=trust \
    postgres:16 >/dev/null

  until docker exec "${POSTGRES_CONTAINER}" pg_isready -U postgres >/dev/null 2>&1; do
    sleep 1
  done
}

start_pgbouncer() {
  docker rm -f "${PGBOUNCER_CONTAINER}" >/dev/null 2>&1 || true
  docker run -d \
    --name "${PGBOUNCER_CONTAINER}" \
    --link "${POSTGRES_CONTAINER}:postgres" \
    -p "${PGBOUNCER_PORT}:5432" \
    -e DATABASE_URL=postgres://postgres:postgres@postgres:5432/postgres \
    -e POOL_MODE=session \
    -e AUTH_TYPE=trust \
    -e MAX_CLIENT_CONN=100 \
    -e DEFAULT_POOL_SIZE=20 \
    -e ADMIN_USERS=postgres \
    -e STATS_USERS=postgres \
    edoburu/pgbouncer:latest >/dev/null

  until bash -c "exec 3<>/dev/tcp/127.0.0.1/${PGBOUNCER_PORT}" >/dev/null 2>&1; do
    sleep 1
  done
}

stop_all() {
  docker rm -f "${PGBOUNCER_CONTAINER}" >/dev/null 2>&1 || true
  docker rm -f "${POSTGRES_CONTAINER}" >/dev/null 2>&1 || true
}

case "${1:-}" in
  start)
    start_postgres
    start_pgbouncer
    ;;
  stop)
    stop_all
    ;;
  *)
    echo "Usage: $0 {start|stop}" >&2
    exit 1
    ;;
esac
