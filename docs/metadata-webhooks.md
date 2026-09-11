# Metadata update webhooks

Public `POST /:entity-id/metadata` enqueues one webhook after the metadata merge
commits. Internal writers (`updateEntityMetadata`, reconciliation, queue
recovery) do not emit these events.

## Events

| Event | When |
| --- | --- |
| `ledger.metadata.updated` | Ledger metadata merged |
| `balance.metadata.updated` | Balance metadata merged |
| `identity.metadata.updated` | Identity metadata merged |
| `transaction.metadata.updated` | Transaction metadata merged |

Each payload includes `event_id` and `timestamp` at the top of `data` for
consumer deduplication.

## Payload shape

**Ledger, balance, identity** — full resource snapshot with merged `meta_data`
for this write. Resource fields sit at the top of `data` (same shape as
`* .created` events).

**Transaction** — patch only:

```json
{
  "event": "transaction.metadata.updated",
  "data": {
    "transaction_id": "bulk_… or txn_…",
    "meta_data_patch": { "key": "value" },
    "event_id": "…",
    "timestamp": "…"
  }
}
```

The DB JSONB-merges `meta_data_patch` onto every matching row. The event does
not re-read merged `meta_data`.

## Consumer notes

- **Bulk IDs (`bulk_…`)** — one event is emitted for the request, but the DB
  update applies to every row where `transaction_id = bulk_id OR
  parent_transaction = bulk_id`. The event's `transaction_id` is the scope ID,
  not a list of child rows.
- **Balance amounts** — `balance.metadata.updated` carries monetary fields as
  read before the merge. They may already be stale; read the balance back if
  position matters.
- **Ordering** — not guaranteed per entity under concurrent updates.
- **`event_id`** — deduplicates asynq redeliveries of the same enqueued payload.
  It does not collapse separate commits or client retries that each produce a
  new event.
- **Delivery guarantee** — the merge is authoritative once the API returns 200.
  Webhook delivery is best-effort: if Redis enqueue fails after commit, the
  notification is permanently dropped (logged and reported via
  `notification.NotifyError`). These events reduce polling for metadata changes
  but do not replace the API for critical workflows that require guaranteed
  notification. A transactional outbox is the upgrade path for lossless
  delivery.
- **Search reindex** — transaction metadata updates reindex Typesense from a
  full row read (including `effective_date` and flags restored from `meta_data`).
  `inflight_expiry_date` is not stored in Postgres today, so it cannot be
  restored on metadata-only reindex (same limitation as full DB reindex).
