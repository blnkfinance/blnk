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

-- Hash-chain columns on transactions. All nullable: existing rows are simply
-- "not yet chained" and the background chainer fills them in.
ALTER TABLE blnk.transactions ADD COLUMN IF NOT EXISTS chain_seq BIGINT;
ALTER TABLE blnk.transactions ADD COLUMN IF NOT EXISTS chain_prev_hash CHAR(64);
ALTER TABLE blnk.transactions ADD COLUMN IF NOT EXISTS chain_hash CHAR(64);

-- Single-row bookmark for the global chain. head_hash/last_seq advance
-- atomically with each chained batch (same DB transaction) — that is the
-- crash-safety mechanism.
CREATE TABLE IF NOT EXISTS blnk.chain_state (
    chain_key    TEXT PRIMARY KEY,
    last_seq     BIGINT    NOT NULL DEFAULT 0,
    head_hash    CHAR(64)  NOT NULL,
    genesis_hash CHAR(64)  NOT NULL,
    genesis_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Seed the single 'global' chain. Genesis = 64 zeros; the head starts there.
INSERT INTO blnk.chain_state (chain_key, head_hash, genesis_hash)
VALUES ('global', repeat('0', 64), repeat('0', 64))
ON CONFLICT (chain_key) DO NOTHING;

-- Verifier walks the chain in order; also forbids two rows sharing a position.
-- This partial index is empty on existing installs (every row starts with a
-- NULL chain_seq), so it builds instantly regardless of table size.
CREATE UNIQUE INDEX IF NOT EXISTS idx_transactions_chain_seq
    ON blnk.transactions (chain_seq) WHERE chain_seq IS NOT NULL;

-- Note: the chainer's scan index (idx_transactions_unchained) covers all rows
-- initially, so it is built CONCURRENTLY in the next migration to avoid locking
-- the transactions table on large installs.

-- +migrate StatementBegin
-- Replace the immutability trigger function with a NULL-safe version. The
-- previous body used `NEW.col = OLD.col`, which is NULL-unsafe: a NULL in any
-- protected column (e.g. scheduled_for) made the whole condition NULL and raised
-- even for benign meta_data updates. This version raises only when a protected
-- column actually changes (IS DISTINCT FROM is NULL-safe), leaving meta_data and
-- the chain_* columns free to be updated.
CREATE OR REPLACE FUNCTION blnk.prevent_transaction_update()
    RETURNS TRIGGER
    LANGUAGE plpgsql
AS $$
BEGIN
    IF (
        NEW.transaction_id IS DISTINCT FROM OLD.transaction_id OR
        NEW.source         IS DISTINCT FROM OLD.source OR
        NEW.destination    IS DISTINCT FROM OLD.destination OR
        NEW.description    IS DISTINCT FROM OLD.description OR
        NEW.reference      IS DISTINCT FROM OLD.reference OR
        NEW.amount         IS DISTINCT FROM OLD.amount OR
        NEW.precise_amount IS DISTINCT FROM OLD.precise_amount OR
        NEW.precision      IS DISTINCT FROM OLD.precision OR
        NEW.currency       IS DISTINCT FROM OLD.currency OR
        NEW.status         IS DISTINCT FROM OLD.status OR
        NEW.hash           IS DISTINCT FROM OLD.hash OR
        NEW.created_at     IS DISTINCT FROM OLD.created_at OR
        NEW.scheduled_for  IS DISTINCT FROM OLD.scheduled_for
    ) THEN
        RAISE EXCEPTION 'Cannot modify transaction data. Only meta_data and chain fields can be updated.';
    END IF;
    RETURN NEW;
END;
$$;
-- +migrate StatementEnd

-- +migrate Down

DROP INDEX IF EXISTS blnk.idx_transactions_chain_seq;
DROP TABLE IF EXISTS blnk.chain_state;
ALTER TABLE blnk.transactions DROP COLUMN IF EXISTS chain_hash;
ALTER TABLE blnk.transactions DROP COLUMN IF EXISTS chain_prev_hash;
ALTER TABLE blnk.transactions DROP COLUMN IF EXISTS chain_seq;

-- +migrate StatementBegin
-- Restore the previous (NULL-unsafe) immutability trigger function.
CREATE OR REPLACE FUNCTION blnk.prevent_transaction_update()
    RETURNS TRIGGER
    LANGUAGE plpgsql
AS $$
BEGIN
    IF (
        NEW.transaction_id = OLD.transaction_id AND
        NEW.source = OLD.source AND
        NEW.destination = OLD.destination AND
        NEW.description = OLD.description AND
        NEW.reference = OLD.reference AND
        NEW.amount = OLD.amount AND
        NEW.currency = OLD.currency AND
        NEW.status = OLD.status AND
        NEW.hash = OLD.hash AND
        NEW.created_at = OLD.created_at AND
        NEW.scheduled_for = OLD.scheduled_for
    ) THEN
        RETURN NEW;
    ELSE
        RAISE EXCEPTION 'Cannot modify transaction data. Only meta_data field can be updated.';
    END IF;
END;
$$;
-- +migrate StatementEnd
