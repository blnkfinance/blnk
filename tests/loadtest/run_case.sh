#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <case-name> [queue-mode]"
  echo "cases: hot-to-cold-no-shard | cold-to-hot-no-shard | hot-to-cold-shard | cold-to-hot-shard"
  echo "queue-mode: normal | spread | hot (default: normal)"
  exit 1
fi

CASE_NAME="$1"
QUEUE_MODE="${2:-normal}"

URL="${URL:-http://localhost:5001/transactions}"
DURATION="${DURATION:-30s}"
RATE="${RATE:-300}"
VUS="${VUS:-200}"
MAX_VUS="${MAX_VUS:-800}"
REDIS_DSN="${BLNK_REDIS_DNS:-${REDIS_DSN:-}}"

OUT_DIR="tests/loadtest"
SUMMARY_OUT="${OUT_DIR}/summary-${CASE_NAME}.json"
NDJSON_OUT="${OUT_DIR}/run-${CASE_NAME}.ndjson"
QUEUE_OUT="${OUT_DIR}/queue-summary-${CASE_NAME}.json"

SCENARIO=""
SOURCE_BUCKETS="1"
DESTINATION_BUCKETS="1"
QUEUE_PREFIXES="new:transaction_"

case "${CASE_NAME}" in
  hot-to-cold-no-shard)
    SCENARIO="hot_source"
    ;;
  cold-to-hot-no-shard)
    SCENARIO="hot_destination"
    ;;
  hot-to-cold-shard)
    SCENARIO="hot_source"
    SOURCE_BUCKETS="${SOURCE_BUCKETS_OVERRIDE:-8}"
    ;;
  cold-to-hot-shard)
    SCENARIO="hot_destination"
    DESTINATION_BUCKETS="${DESTINATION_BUCKETS_OVERRIDE:-8}"
    ;;
  *)
    echo "unknown case: ${CASE_NAME}"
    exit 1
    ;;
esac

if [[ "${QUEUE_MODE}" == "hot" ]]; then
  QUEUE_PREFIXES="new:transaction_,hot_"
fi

if [[ -z "${REDIS_DSN}" ]]; then
  echo "BLNK_REDIS_DNS or REDIS_DSN is required for queue benchmarking"
  exit 1
fi

echo "Starting queue benchmark for ${CASE_NAME}"
go run ./tests/loadtest/tools/queue_benchmark.go \
  -redis-dsn "${REDIS_DSN}" \
  -queue-prefixes "${QUEUE_PREFIXES}" \
  -wait \
  -out "${QUEUE_OUT}" &
QUEUE_BENCH_PID=$!

cleanup() {
  if kill -0 "${QUEUE_BENCH_PID}" >/dev/null 2>&1; then
    kill -INT "${QUEUE_BENCH_PID}" >/dev/null 2>&1 || true
    wait "${QUEUE_BENCH_PID}" >/dev/null 2>&1 || true
  fi
}

trap cleanup EXIT

echo "Running ${CASE_NAME}"
k6 run \
  --out "json=${NDJSON_OUT}" \
  -e SUMMARY_OUT="${SUMMARY_OUT}" \
  -e URL="${URL}" \
  -e SCENARIO="${SCENARIO}" \
  -e DURATION="${DURATION}" \
  -e RATE="${RATE}" \
  -e VUS="${VUS}" \
  -e MAX_VUS="${MAX_VUS}" \
  -e SOURCE_BUCKETS="${SOURCE_BUCKETS}" \
  -e DESTINATION_BUCKETS="${DESTINATION_BUCKETS}" \
  tests/loadtest/script.js

echo "Waiting for queue drain benchmark to finish"
wait "${QUEUE_BENCH_PID}"

trap - EXIT

echo "Generated files:"
echo "  ${SUMMARY_OUT}"
echo "  ${NDJSON_OUT}"
echo "  ${QUEUE_OUT}"
