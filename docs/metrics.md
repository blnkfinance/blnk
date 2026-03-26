# Blnk Metrics Reference

Blnk exposes OpenTelemetry metrics via a Prometheus-compatible `/metrics` endpoint. This document lists all available metrics, their types, attributes, and what they tell you operationally.

## Prerequisites

- Set `"enable_observability": true` in `blnk.json` (or `BLNK_ENABLE_OBSERVABILITY=true`)
- Metrics are served on:
  - **Server**: `GET /metrics` on the API port (default `5001`)
  - **Worker**: `GET /metrics` on the monitoring port (default `5004`)

## Authentication

When `server.secure` is enabled, the `/metrics` endpoint requires a bearer token:

1. Set `"metrics_bearer_token": "<your-token>"` in `blnk.json` (or `BLNK_METRICS_BEARER_TOKEN`)
2. Configure Prometheus to send the token:

```yaml
scrape_configs:
  - job_name: 'blnk-server'
    authorization:
      type: Bearer
      credentials: '<your-token>'
    static_configs:
      - targets: ['server:5001']
```

If secure mode is enabled without a token configured, the endpoint returns `403 Forbidden`.

## Export Modes

| Mode | How it works | When active |
|------|-------------|-------------|
| **Pull (Prometheus)** | Prometheus scrapes `/metrics` | Always (when observability is enabled) |
| **Push (OTLP HTTP)** | Periodically pushes to an OTel Collector | When `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` or `OTEL_EXPORTER_OTLP_ENDPOINT` is set |

## Metrics

> **Naming convention**: OTel instrument names use dots (e.g., `blnk.transaction.total`). The Prometheus exporter converts these to underscores automatically (e.g., `blnk_transaction_total`).

### Transaction Metrics

| Prometheus Name | Type | Attributes | Description |
|----------------|------|------------|-------------|
| `blnk_transaction_total` | Counter | `status`, `currency` | Total transactions by final status and currency. Use to track throughput and status distribution. |
| `blnk_transaction_duration_seconds` | Histogram | `status` | Wall-clock time of `RecordTransaction()` in seconds. Use to monitor processing latency and set SLOs. |
| `blnk_transaction_rejected_total` | Counter | `reason` | Rejected transactions by reason. Use to alert on elevated rejection rates. |

**`status` values**: `APPLIED`, `REJECTED`, `INFLIGHT`, `VOID`, `COMMIT`

**`reason` values**: `insufficient_funds`, `overdraft_limit`, `lock_contention`, `max_retries`, `other`

**`currency` values**: Determined by the transactions processed (e.g., `USD`, `NGN`, `EUR`)

### Queue Metrics

| Prometheus Name | Type | Attributes | Description |
|----------------|------|------------|-------------|
| `blnk_queue_enqueued_total` | Counter | `queue_name` | Transactions enqueued for async processing. Use to monitor inflow rate per queue. |
| `blnk_queue_processing_duration_seconds` | Histogram | `result` | Time spent processing a transaction in the worker. Use to detect worker slowdowns. |

**`queue_name` values**: `new:transaction_1` through `new:transaction_N` (sharded queues), `hot_transactions` (hot lane)

**`result` values**: `success`

### Balance Metrics

| Prometheus Name | Type | Attributes | Description |
|----------------|------|------------|-------------|
| `blnk_balance_created_total` | Counter | — | Total balances created. Use to track account growth. |

### Inflight Transaction Metrics

| Prometheus Name | Type | Attributes | Description |
|----------------|------|------------|-------------|
| `blnk_inflight_commit_total` | Counter | — | Inflight transactions committed. |
| `blnk_inflight_void_total` | Counter | — | Inflight transactions voided. Use the commit/void ratio to monitor authorization patterns. |

### Batch Coalescing Metrics

| Prometheus Name | Type | Attributes | Description |
|----------------|------|------------|-------------|
| `blnk_transaction_batch_size` | Histogram | — | Number of transactions per coalesced batch. Use to evaluate batching efficiency. |
| `blnk_transaction_batch_total` | Counter | `result` | Coalescing attempts by outcome. |

**`result` values**: `success`, `failure`, `skipped`

### Hot Pairs / Lock Contention Metrics

| Prometheus Name | Type | Attributes | Description |
|----------------|------|------------|-------------|
| `blnk_hotpairs_contention_total` | Counter | — | Lock acquisition failures. Spikes indicate contention on specific balance pairs. |
| `blnk_hotpairs_lane_routed_total` | Counter | `lane` | Transactions routed to queue lanes. Use to verify hot lane is activating as expected. |

**`lane` values**: `normal`, `hot`

### Worker Metrics

| Prometheus Name | Type | Attributes | Description |
|----------------|------|------------|-------------|
| `blnk_worker_retries_total` | Counter | `reason` | Worker retry events. Sustained retries indicate systemic issues. |

**`reason` values**: `insufficient_funds`, `other`

## Example Prometheus Queries

```promql
# Transaction throughput (per second, 5 minute window)
rate(blnk_transaction_total[5m])

# Rejection rate by reason
rate(blnk_transaction_rejected_total[5m])

# P99 transaction latency
histogram_quantile(0.99, rate(blnk_transaction_duration_seconds_bucket[5m]))

# Queue enqueue rate
rate(blnk_queue_enqueued_total[5m])

# Hot lane traffic ratio
blnk_hotpairs_lane_routed_total{lane="hot"} / blnk_hotpairs_lane_routed_total

# Worker retry rate
rate(blnk_worker_retries_total[5m])

# Batch coalescing success rate
blnk_transaction_batch_total{result="success"} / blnk_transaction_batch_total
```
