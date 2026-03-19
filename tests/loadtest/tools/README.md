# Load Test Dashboard

This replaces the one-off conversion workflow with a small local web app.

For the end-to-end load-test workflow, named test cases, and queue benchmark usage, see:

- `tests/loadtest/README.md`

## Start the dashboard

```bash
python3 tests/loadtest/tools/dashboard.py
```

That starts a local server, opens the dashboard in your browser, and scans `tests/loadtest/` for:

- k6 `summary.json` files
- raw k6 NDJSON exports
- CSV files produced by the old minute-bucket flow

## Optional flags

```bash
python3 tests/loadtest/tools/dashboard.py --no-open
python3 tests/loadtest/tools/dashboard.py --port 9000
python3 tests/loadtest/tools/dashboard.py --root tests/loadtest
```

## Queue drain benchmark

For temporary queue-drain benchmarking, generate a k6-shaped summary JSON from Asynq queue stats:

```bash
go run ./tests/loadtest/tools/queue_benchmark.go \
  -queue-prefix new:transaction_ \
  -wait \
  -out tests/loadtest/queue-summary.json
```

Or target one specific queue:

```bash
go run ./tests/loadtest/tools/queue_benchmark.go \
  -queues new:transaction_18 \
  -out tests/loadtest/queue-summary.json
```

If you are not loading the full app config, pass Redis directly:

```bash
go run ./tests/loadtest/tools/queue_benchmark.go \
  -redis-dsn "$BLNK_REDIS_DNS" \
  -queue-prefix new:transaction_ \
  -wait \
  -out tests/loadtest/queue-summary.json
```

Useful flags:

- `-config <path>`: load Blnk config from a specific file before reading env vars
- `-interval 1s`: polling cadence
- `-timeout 15m`: maximum drain benchmark duration
- `-wait`: wait for backlog to appear before starting measurement

## What the UI supports

- Discovered load-test runs from disk
- Drag-and-drop upload of JSON, NDJSON, or CSV load-test files
- KPI cards for throughput, latency, success, Apdex, duration, and VUs
- Threshold and check summaries
- Simple trend charts when minute-level data exists
