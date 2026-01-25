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

-- +migrate Up

-- Create lineage_outbox table for atomic lineage processing (if not exists)
-- This table captures lineage intent within the same transaction as the main transaction,
-- ensuring no lineage work is lost even if Redis/queue operations fail.
CREATE TABLE IF NOT EXISTS blnk.lineage_outbox (
    id BIGSERIAL PRIMARY KEY,
    transaction_id TEXT NOT NULL,
    source_balance_id TEXT,
    destination_balance_id TEXT,
    provider TEXT,
    lineage_type TEXT NOT NULL,  -- 'credit', 'debit', 'both'
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 5,
    last_error TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMP,
    locked_until TIMESTAMP,
    inflight BOOLEAN NOT NULL DEFAULT FALSE
);

-- Add unique constraint on transaction_id if it doesn't exist
CREATE UNIQUE INDEX IF NOT EXISTS lineage_outbox_transaction_id_uidx
    ON blnk.lineage_outbox (transaction_id);
    
-- Partial index for efficient polling of pending entries
CREATE INDEX IF NOT EXISTS idx_lineage_outbox_pending
    ON blnk.lineage_outbox(status, created_at)
    WHERE status = 'pending';

-- Index for looking up outbox entries by transaction
CREATE INDEX IF NOT EXISTS idx_lineage_outbox_transaction
    ON blnk.lineage_outbox(transaction_id);

-- Index for looking up failed entries for monitoring
CREATE INDEX IF NOT EXISTS idx_lineage_outbox_failed
    ON blnk.lineage_outbox(status, created_at)
    WHERE status = 'failed';

-- Composite index for efficient outbox polling with retry support
CREATE INDEX IF NOT EXISTS idx_lineage_outbox_claim
    ON blnk.lineage_outbox(status, locked_until, attempts, created_at)
    WHERE status IN ('pending', 'processing');

-- Add foreign key constraints on lineage_mappings
-- Using ON DELETE CASCADE so mappings are cleaned up when balances are deleted
ALTER TABLE blnk.lineage_mappings
    ADD CONSTRAINT fk_lineage_mappings_balance_id
    FOREIGN KEY (balance_id) REFERENCES blnk.balances(balance_id) ON DELETE CASCADE;

ALTER TABLE blnk.lineage_mappings
    ADD CONSTRAINT fk_lineage_mappings_shadow_balance_id
    FOREIGN KEY (shadow_balance_id) REFERENCES blnk.balances(balance_id) ON DELETE CASCADE;

ALTER TABLE blnk.lineage_mappings
    ADD CONSTRAINT fk_lineage_mappings_aggregate_balance_id
    FOREIGN KEY (aggregate_balance_id) REFERENCES blnk.balances(balance_id) ON DELETE CASCADE;

ALTER TABLE blnk.lineage_mappings
    ADD CONSTRAINT fk_lineage_mappings_identity_id
    FOREIGN KEY (identity_id) REFERENCES blnk.identity(identity_id) ON DELETE CASCADE;

-- +migrate Down

-- Remove foreign key constraints from lineage_mappings
ALTER TABLE blnk.lineage_mappings DROP CONSTRAINT IF EXISTS fk_lineage_mappings_balance_id;
ALTER TABLE blnk.lineage_mappings DROP CONSTRAINT IF EXISTS fk_lineage_mappings_shadow_balance_id;
ALTER TABLE blnk.lineage_mappings DROP CONSTRAINT IF EXISTS fk_lineage_mappings_aggregate_balance_id;
ALTER TABLE blnk.lineage_mappings DROP CONSTRAINT IF EXISTS fk_lineage_mappings_identity_id;

-- Drop indexes
DROP INDEX IF EXISTS blnk.idx_lineage_outbox_claim;
DROP INDEX IF EXISTS blnk.idx_lineage_outbox_pending;
DROP INDEX IF EXISTS blnk.idx_lineage_outbox_transaction;
DROP INDEX IF EXISTS blnk.idx_lineage_outbox_failed;

-- Remove unique constraint
ALTER TABLE blnk.lineage_outbox DROP CONSTRAINT IF EXISTS lineage_outbox_transaction_id_key;
