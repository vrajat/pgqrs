#!/usr/bin/env bash
set -euo pipefail

# Usage: ./run_benchmark_with_wal.sh [consumers] [producers] [duration]
# Example: SCENARIO=queue_logged_skiplocked ./run_benchmark_with_wal.sh 20 5 2m

CONSUMERS=${1:-10}
PRODUCERS=${2:-2}
DURATION=${3:-1m}
SCENARIO=${SCENARIO:-queue_logged_skiplocked}
STOP_TIMEOUT=${STOP_TIMEOUT:-10s}
BATCH_SIZE=${BATCH_SIZE:-10}
RESULT_DIR=results/$(date +%Y%m%d-%H%M%S)-${SCENARIO}-${CONSUMERS}c-${PRODUCERS}p
mkdir -p "${RESULT_DIR}"

echo "Running benchmark with WAL capture"
echo "Scenario: ${SCENARIO}; Consumers: ${CONSUMERS}; Producers: ${PRODUCERS}; Duration: ${DURATION}; Batch: ${BATCH_SIZE}"
echo "Results directory: ${RESULT_DIR}"

# Postgres connection params (can be overridden by env)
PG_HOST=${PG_HOST:-localhost}
PG_PORT=${PG_PORT:-5432}
PG_USER=${PG_USER:-pgbench}
PG_PASS=${PG_PASS:-pgbench}
PG_DB=${PG_DB:-pgbench}

export PGPASSWORD="${PG_PASS}"

PSQL_BASE="psql -h ${PG_HOST} -p ${PG_PORT} -U ${PG_USER} -d ${PG_DB} -t -A -q -c"

# Check psql exists
if ! command -v psql >/dev/null 2>&1; then
  echo "psql not found in PATH. Please install psql client." >&2
  exit 1
fi

# record start WAL LSN
echo "Recording start WAL LSN..."
START_LSN=$($PSQL_BASE "select pg_current_wal_lsn();" | tr -d '[:space:]')
if [ -z "$START_LSN" ]; then
  echo "Failed to read start LSN" >&2
  exit 1
fi
echo "Start LSN: $START_LSN" | tee "${RESULT_DIR}/wal_capture.log"

# Start iostat (background) - handles Darwin and Linux
echo "Starting iostat to ${RESULT_DIR}/iostat.log (background)"
if command -v iostat >/dev/null 2>&1; then
  UNAME=$(uname)
  if [ "$UNAME" = "Darwin" ]; then
    # macOS: `iostat -d 1` prints device stats every second
    iostat -d 1 > "${RESULT_DIR}/iostat.log" 2>&1 &
    IOSTAT_PID=$!
  else
    # Linux: extended stats
    iostat -x 1 > "${RESULT_DIR}/iostat.log" 2>&1 &
    IOSTAT_PID=$!
  fi
else
  echo "iostat not found; continuing without iostat capture" | tee -a "${RESULT_DIR}/iostat.log"
  IOSTAT_PID=""
fi

# Export scenario and batch size for locust
export SCENARIO
export BATCH_SIZE

# Run locust headless (same parameters used in harness). Output redirected to files.
TOTAL_USERS=$((CONSUMERS + PRODUCERS))
SPAWN_RATE=${SPAWN_RATE:-5}
LOCUST_FILE=${LOCUST_FILE:-locustfile.py}

echo "Starting locust (headless) - writing stdout/stderr to ${RESULT_DIR}"

locust -f "${LOCUST_FILE}" --headless -u ${TOTAL_USERS} -r ${SPAWN_RATE} --run-time ${DURATION} --stop-timeout ${STOP_TIMEOUT} --csv "${RESULT_DIR}/locust" --only-summary > "${RESULT_DIR}/locust_stdout.log" 2> "${RESULT_DIR}/locust_stderr.log" || true

# After locust completes, record end LSN
echo "Locust run completed. Recording end WAL LSN..."
END_LSN=$($PSQL_BASE "select pg_current_wal_lsn();" | tr -d '[:space:]')
if [ -z "$END_LSN" ]; then
  echo "Failed to read end LSN" >&2
  # continue but note failure
  echo "END_LSN=FAILED" >> "${RESULT_DIR}/wal_capture.log"
else
  echo "End LSN: $END_LSN" | tee -a "${RESULT_DIR}/wal_capture.log"
  # compute WAL bytes difference
  WAL_BYTES=$($PSQL_BASE "select pg_wal_lsn_diff('${END_LSN}', '${START_LSN}');" | tr -d '[:space:]' )
  echo "WAL bytes generated during run: ${WAL_BYTES}" | tee -a "${RESULT_DIR}/wal_capture.log"
fi

# Stop iostat if running
if [ -n "${IOSTAT_PID}" ] && [ "${IOSTAT_PID}" != "" ]; then
  echo "Stopping iostat (pid ${IOSTAT_PID})"
  kill ${IOSTAT_PID} || true
fi

# Save run metadata
cat > "${RESULT_DIR}/run_metadata.txt" <<EOF
scenario=${SCENARIO}
consumers=${CONSUMERS}
producers=${PRODUCERS}
duration=${DURATION}
batch_size=${BATCH_SIZE}
start_lsn=${START_LSN}
end_lsn=${END_LSN}
wal_bytes=${WAL_BYTES:-UNKNOWN}
locust_stdout=${RESULT_DIR}/locust_stdout.log
locust_stderr=${RESULT_DIR}/locust_stderr.log
iostat_log=${RESULT_DIR}/iostat.log
wal_capture_log=${RESULT_DIR}/wal_capture.log
EOF

echo "Benchmark run complete. Results saved to ${RESULT_DIR}"
