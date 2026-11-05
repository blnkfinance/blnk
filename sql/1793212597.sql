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

-- Record which transactions a balance snapshot contains (#398).
--
-- last_tx_seq is the highest APPLIED blnk.transactions.id on the balance when
-- the snapshot was taken. Transactions are inserted after the balance update in
-- the same database transaction, so per balance their ids follow commit order
-- and id <= last_tx_seq means "already in the snapshot", whatever the
-- effective_date. Older snapshots keep it NULL.

-- +migrate Up
ALTER TABLE blnk.balance_snapshots ADD COLUMN IF NOT EXISTS last_tx_seq BIGINT;

-- +migrate StatementBegin
CREATE OR REPLACE FUNCTION blnk.take_daily_balance_snapshots_batched(batch_size INTEGER DEFAULT 1000)
    RETURNS INTEGER
    LANGUAGE plpgsql
    STRICT
AS $$
DECLARE
    v_last_processed_id BIGINT := 0;
    v_max_id BIGINT;
    v_total_processed INTEGER := 0;
    v_batch_count INTEGER;
    v_current_time TIMESTAMP;
BEGIN
    -- Store the current time to use consistently throughout the function
    v_current_time := NOW();

    -- Get the maximum ID first to know when to stop
    SELECT MAX(id) INTO v_max_id FROM blnk.balances;

    LOOP
        -- One statement, so the balance row and last_tx_seq come from the same
        -- view of the database.
        WITH batch AS (
            SELECT
                b.id,
                b.balance_id,
                b.ledger_id,
                b.balance,
                b.credit_balance,
                b.debit_balance,
                b.inflight_balance,
                b.inflight_credit_balance,
                b.inflight_debit_balance,
                b.currency,
                b.created_at,
                b.meta_data
            FROM blnk.balances b
            WHERE b.id > v_last_processed_id
            AND NOT EXISTS (
                SELECT 1
                FROM blnk.balance_snapshots bs
                WHERE bs.balance_id = b.balance_id
                AND DATE_TRUNC('day', bs.snapshot_time) = DATE_TRUNC('day', v_current_time)
            )
            ORDER BY b.id
            LIMIT batch_size
        )
        INSERT INTO blnk.balance_snapshots (
            balance_id,
            ledger_id,
            balance,
            credit_balance,
            debit_balance,
            inflight_balance,
            inflight_credit_balance,
            inflight_debit_balance,
            currency,
            snapshot_time,
            created_at,
            last_tx_id,
            last_tx_seq,
            meta_data
        )
        SELECT
            b.balance_id,
            b.ledger_id,
            b.balance,
            b.credit_balance,
            b.debit_balance,
            b.inflight_balance,
            b.inflight_credit_balance,
            b.inflight_debit_balance,
            b.currency,
            v_current_time,
            v_current_time,
            (
                SELECT t.transaction_id
                FROM blnk.transactions t
                WHERE t.source = b.balance_id OR t.destination = b.balance_id
                ORDER BY t.created_at DESC
                LIMIT 1
            ),
            COALESCE((
                SELECT MAX(t.id)
                FROM blnk.transactions t
                WHERE (t.source = b.balance_id OR t.destination = b.balance_id)
                AND t.status = 'APPLIED'
            ), 0),
            b.meta_data
        FROM batch b;

        GET DIAGNOSTICS v_batch_count = ROW_COUNT;
        v_total_processed := v_total_processed + v_batch_count;

        -- Get the last ID processed in this batch
        SELECT MAX(id) INTO v_last_processed_id
        FROM (
            SELECT id
            FROM blnk.balances
            WHERE id > v_last_processed_id
            ORDER BY id
            LIMIT batch_size
        ) AS latest_batch;

        -- Exit if we've processed everything or no more records to process
        EXIT WHEN v_batch_count = 0 OR v_last_processed_id >= v_max_id;
    END LOOP;

    RETURN v_total_processed;
END;
$$;
-- +migrate StatementEnd

-- +migrate StatementBegin
CREATE OR REPLACE FUNCTION blnk.prevent_snapshot_update()
   RETURNS TRIGGER
   LANGUAGE plpgsql
AS $$
BEGIN
   -- Allow updates to meta_data column only
   IF (NEW.meta_data IS DISTINCT FROM OLD.meta_data) AND (
       NEW.balance_id = OLD.balance_id AND
       NEW.ledger_id = OLD.ledger_id AND
       NEW.balance = OLD.balance AND
       NEW.credit_balance = OLD.credit_balance AND
       NEW.debit_balance = OLD.debit_balance AND
       NEW.inflight_balance = OLD.inflight_balance AND
       NEW.inflight_credit_balance = OLD.inflight_credit_balance AND
       NEW.inflight_debit_balance = OLD.inflight_debit_balance AND
       NEW.currency = OLD.currency AND
       NEW.snapshot_time = OLD.snapshot_time AND
       NEW.created_at = OLD.created_at AND
       NEW.last_tx_id = OLD.last_tx_id AND
       NEW.last_tx_seq IS NOT DISTINCT FROM OLD.last_tx_seq
   ) THEN
       RETURN NEW;
   ELSE
       RAISE EXCEPTION 'Cannot modify balance snapshot data. Only meta_data field can be updated.';
   END IF;
END;
$$;
-- +migrate StatementEnd

-- +migrate Down

-- +migrate StatementBegin
CREATE OR REPLACE FUNCTION blnk.prevent_snapshot_update()
   RETURNS TRIGGER
   LANGUAGE plpgsql
AS $$
BEGIN
   -- Allow updates to meta_data column only
   IF (NEW.meta_data IS DISTINCT FROM OLD.meta_data) AND (
       NEW.balance_id = OLD.balance_id AND
       NEW.ledger_id = OLD.ledger_id AND
       NEW.balance = OLD.balance AND
       NEW.credit_balance = OLD.credit_balance AND
       NEW.debit_balance = OLD.debit_balance AND
       NEW.inflight_balance = OLD.inflight_balance AND
       NEW.inflight_credit_balance = OLD.inflight_credit_balance AND
       NEW.inflight_debit_balance = OLD.inflight_debit_balance AND
       NEW.currency = OLD.currency AND
       NEW.snapshot_time = OLD.snapshot_time AND
       NEW.created_at = OLD.created_at AND
       NEW.last_tx_id = OLD.last_tx_id
   ) THEN
       RETURN NEW;
   ELSE
       RAISE EXCEPTION 'Cannot modify balance snapshot data. Only meta_data field can be updated.';
   END IF;
END;
$$;
-- +migrate StatementEnd

-- +migrate StatementBegin
CREATE OR REPLACE FUNCTION blnk.take_daily_balance_snapshots_batched(batch_size INTEGER DEFAULT 1000)
    RETURNS INTEGER
    LANGUAGE plpgsql
    STRICT
AS $$
DECLARE
    v_last_processed_id BIGINT := 0;
    v_max_id BIGINT;
    v_total_processed INTEGER := 0;
    v_batch_count INTEGER;
    v_current_time TIMESTAMP;
BEGIN
    v_current_time := NOW();

    SELECT MAX(id) INTO v_max_id FROM blnk.balances;

    LOOP
        WITH batch AS (
            SELECT
                b.id,
                b.balance_id,
                b.ledger_id,
                b.balance,
                b.credit_balance,
                b.debit_balance,
                b.inflight_balance,
                b.inflight_credit_balance,
                b.inflight_debit_balance,
                b.currency,
                b.created_at,
                b.meta_data
            FROM blnk.balances b
            WHERE b.id > v_last_processed_id
            AND NOT EXISTS (
                SELECT 1
                FROM blnk.balance_snapshots bs
                WHERE bs.balance_id = b.balance_id
                AND DATE_TRUNC('day', bs.snapshot_time) = DATE_TRUNC('day', v_current_time)
            )
            ORDER BY b.id
            LIMIT batch_size
        )
        INSERT INTO blnk.balance_snapshots (
            balance_id,
            ledger_id,
            balance,
            credit_balance,
            debit_balance,
            inflight_balance,
            inflight_credit_balance,
            inflight_debit_balance,
            currency,
            snapshot_time,
            created_at,
            last_tx_id,
            meta_data
        )
        SELECT
            b.balance_id,
            b.ledger_id,
            b.balance,
            b.credit_balance,
            b.debit_balance,
            b.inflight_balance,
            b.inflight_credit_balance,
            b.inflight_debit_balance,
            b.currency,
            v_current_time,
            v_current_time,
            (
                SELECT t.transaction_id
                FROM blnk.transactions t
                WHERE t.source = b.balance_id OR t.destination = b.balance_id
                ORDER BY t.created_at DESC
                LIMIT 1
            ),
            b.meta_data
        FROM batch b;

        GET DIAGNOSTICS v_batch_count = ROW_COUNT;
        v_total_processed := v_total_processed + v_batch_count;

        SELECT MAX(id) INTO v_last_processed_id
        FROM (
            SELECT id
            FROM blnk.balances
            WHERE id > v_last_processed_id
            ORDER BY id
            LIMIT batch_size
        ) AS latest_batch;

        EXIT WHEN v_batch_count = 0 OR v_last_processed_id >= v_max_id;
    END LOOP;

    RETURN v_total_processed;
END;
$$;
-- +migrate StatementEnd

ALTER TABLE blnk.balance_snapshots DROP COLUMN IF EXISTS last_tx_seq;
