-- Copyright 2024 Blnk Finance Authors.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Performance indexes for hot-path queries.
-- Uses CREATE INDEX CONCURRENTLY (requires notransaction) to avoid locking
-- writes on large transactions tables. If a concurrent build is interrupted,
-- it may leave an INVALID index; drop it and re-run the migration.

-- +migrate Up notransaction

-- Coalescing queries (pair/source scope) and GetQueuedAmounts debit branch:
-- WHERE status = 'QUEUED' AND source = $1 [AND currency = $2 AND created_at >= $3]
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_txn_queued_source
ON blnk.transactions (source, currency, created_at)
WHERE status = 'QUEUED';

-- Coalescing queries (destination scope) and GetQueuedAmounts credit branch:
-- WHERE status = 'QUEUED' AND destination = $1 [AND currency = $2 AND created_at >= $3]
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_txn_queued_destination
ON blnk.transactions (destination, currency, created_at)
WHERE status = 'QUEUED';

-- NOT EXISTS child-status checks in GetQueuedAmounts and inflight lookups:
-- WHERE child.parent_transaction = t.transaction_id AND child.status IN (...)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_txn_parent_status
ON blnk.transactions (parent_transaction, status);

-- GetTransactionsByParent sort elimination:
-- WHERE parent_transaction = $1 ORDER BY created_at DESC
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_txn_parent_created
ON blnk.transactions (parent_transaction, created_at DESC);

-- JSONB key lookups in GetInflightTransactionsByParentID / GetRefundableTransactionsByParentID:
-- WHERE meta_data->>'QUEUED_PARENT_TRANSACTION' = $1 (the GIN index does not serve ->> equality)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_txn_meta_queued_parent
ON blnk.transactions ((meta_data->>'QUEUED_PARENT_TRANSACTION'))
WHERE meta_data->>'QUEUED_PARENT_TRANSACTION' IS NOT NULL;

-- Queued-inflight filter in GetInflightTransactionsByParentID:
-- AND status = 'QUEUED' AND meta_data->>'inflight' = 'true'
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_txn_meta_inflight
ON blnk.transactions ((meta_data->>'inflight'))
WHERE meta_data->>'inflight' = 'true';

-- Shadow transaction lookups in GetTransactionsByShadowFor:
-- WHERE meta_data->>'_shadow_for' = $1 ORDER BY created_at ASC
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_txn_meta_shadow_for
ON blnk.transactions ((meta_data->>'_shadow_for'), created_at)
WHERE meta_data->>'_shadow_for' IS NOT NULL;

-- Exact duplicate: blnk_txn_currency_created_at_desc_idx has identical key
-- columns (currency, created_at DESC) plus INCLUDE (precision).
-- Zero risk — pure duplicate from sql/1768315575.sql.
DROP INDEX CONCURRENTLY IF EXISTS blnk.blnk_transactions_currency_created_at_idx;

-- Subsumed by idx_transactions_reference_unique (same column; the unique
-- index serves every read the non-unique one could).
DROP INDEX CONCURRENTLY IF EXISTS blnk.idx_transactions_reference;

-- +migrate Down notransaction

DROP INDEX CONCURRENTLY IF EXISTS blnk.idx_txn_meta_shadow_for;
DROP INDEX CONCURRENTLY IF EXISTS blnk.idx_txn_meta_inflight;
DROP INDEX CONCURRENTLY IF EXISTS blnk.idx_txn_meta_queued_parent;
DROP INDEX CONCURRENTLY IF EXISTS blnk.idx_txn_parent_created;
DROP INDEX CONCURRENTLY IF EXISTS blnk.idx_txn_parent_status;
DROP INDEX CONCURRENTLY IF EXISTS blnk.idx_txn_queued_destination;
DROP INDEX CONCURRENTLY IF EXISTS blnk.idx_txn_queued_source;
