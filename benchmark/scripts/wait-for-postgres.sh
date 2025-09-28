#!/usr/bin/env bash
set -e
HOST=${PG_HOST:-localhost}
PORT=${PG_PORT:-5432}
echo "Waiting for Postgres at $HOST:$PORT..."
until pg_isready -h $HOST -p $PORT -U pgbench; do
  sleep 1
done
echo "Postgres is ready."
