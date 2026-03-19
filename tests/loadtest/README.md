# Load Test Guide

This directory supports two kinds of measurement:

- server/API acceptance via k6
- queue drain time via the queue benchmark tool

Use both. A fast `summary.json` only tells you the API accepted work quickly. The queue summary tells you how long workers took to finish it.

## Open the dashboard

From the repo root:

```bash
python3 tests/loadtest/tools/dashboard.py
```

That starts the dashboard server, opens your browser, and scans `tests/loadtest/` for:

- `summary*.json`
- `queue-summary*.json`
- `run*.ndjson`

If you do not want it to open a browser automatically:

```bash
python3 tests/loadtest/tools/dashboard.py --no-open
```

Open:

```text
http://127.0.0.1:8765/dashboard/
```

## Test cases

These are the main flow tests to compare:

- `hot-to-cold-no-shard`
  - one hot source sends to many cold destinations
- `cold-to-hot-no-shard`
  - many cold sources send to one hot destination
- `hot-to-cold-shard`
  - same as above, but the hot source is split into source buckets first
- `cold-to-hot-shard`
  - same as above, but the hot destination is split into destination buckets first

## What the k6 script supports

`tests/loadtest/script.js` now has explicit scenarios for:

- `hot_source`
- `hot_destination`

And supports optional sharding with:

- `SOURCE_BUCKETS`
- `DESTINATION_BUCKETS`

## Fastest way to run a case

Use the helper script from the repo root:

```bash
bash tests/loadtest/run_case.sh hot-to-cold-no-shard
```

That will:

1. start the queue benchmark
2. run the k6 load test
3. wait for the queue benchmark to finish
4. write three files into `tests/loadtest/`

The output files are:

- `summary-<case>.json`
- `run-<case>.ndjson`
- `queue-summary-<case>.json`

Examples:

```bash
bash tests/loadtest/run_case.sh hot-to-cold-no-shard
bash tests/loadtest/run_case.sh cold-to-hot-no-shard
bash tests/loadtest/run_case.sh hot-to-cold-shard
bash tests/loadtest/run_case.sh cold-to-hot-shard
```

If the hot queue is enabled and you want the queue benchmark to include it:

```bash
bash tests/loadtest/run_case.sh hot-to-cold-no-shard hot
```

That makes the queue benchmark watch both:

- `new:transaction_*`
- `hot_*`

## Manual run flow

If you want to run the tools manually instead of using `run_case.sh`, use two terminals.

Terminal 1, start the queue benchmark:

```bash
go run ./tests/loadtest/tools/queue_benchmark.go \
  -redis-dsn "$BLNK_REDIS_DNS" \
  -queue-prefixes "new:transaction_,hot_" \
  -wait \
  -out tests/loadtest/queue-summary.json
```

Terminal 2, run k6:

```bash
k6 run \
  --out json=tests/loadtest/run.ndjson \
  -e SUMMARY_OUT=tests/loadtest/summary.json \
  -e URL='http://localhost:5001/transactions' \
  -e SCENARIO='hot_source' \
  -e DURATION='30s' \
  -e RATE='300' \
  -e VUS='200' \
  -e MAX_VUS='800' \
  -e SOURCE_BUCKETS='1' \
  tests/loadtest/script.js
```

For `cold-to-hot`:

```bash
k6 run \
  --out json=tests/loadtest/run.ndjson \
  -e SUMMARY_OUT=tests/loadtest/summary.json \
  -e URL='http://localhost:5001/transactions' \
  -e SCENARIO='hot_destination' \
  -e DURATION='30s' \
  -e RATE='300' \
  -e VUS='200' \
  -e MAX_VUS='800' \
  -e DESTINATION_BUCKETS='1' \
  tests/loadtest/script.js
```

To rerun the same test with sharding:

- for `hot_source`, raise `SOURCE_BUCKETS`
- for `hot_destination`, raise `DESTINATION_BUCKETS`

Example:

```bash
k6 run \
  --out json=tests/loadtest/run.ndjson \
  -e SUMMARY_OUT=tests/loadtest/summary.json \
  -e URL='http://localhost:5001/transactions' \
  -e SCENARIO='hot_source' \
  -e DURATION='30s' \
  -e RATE='300' \
  -e VUS='200' \
  -e MAX_VUS='800' \
  -e SOURCE_BUCKETS='8' \
  tests/loadtest/script.js
```

## Suggested comparison order

Use this sequence and keep all generated files:

1. run `hot-to-cold-no-shard`
2. run `cold-to-hot-no-shard`
3. review queue drain time and keep the reports
4. shard and rerun the same two cases
5. compare the new reports against the no-shard runs
6. if you want to test queue optimizations on top, turn on worker concurrency, coalescing, or hot lane and rerun the sharded cases

## What to look at in the dashboard

For each case, inspect both:

- server summary
  - `summary-<case>.json`
- queue summary
  - `queue-summary-<case>.json`

Read them like this:

- `summary`
  - how fast the API accepted the transactions
- `queue-summary`
  - how fast the workers drained the queued work

The most useful comparison fields are:

- throughput
- p95 latency
- duration
- total processed

If the server numbers are good but queue duration is still high, the bottleneck is in worker drain, not API acceptance.
