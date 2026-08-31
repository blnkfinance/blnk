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

-- Pin the snapshot writer's clock to UTC.
--
-- The previous migration aligned snapshot_time's TYPE with the columns it is
-- compared against. That removes the session-timezone coercion Postgres was
-- applying at read time, but it does not by itself say WHICH clock the value
-- is recorded on -- and the two writers disagreed:
--
--   blnk.transactions.created_at   Go inserts txn.CreatedAt.UTC(), so the
--                                  column always holds a UTC wall clock.
--   blnk.balance_snapshots         this function assigns NOW() to a plain
--                                  TIMESTAMP, which Postgres resolves in the
--                                  SESSION's TimeZone -- a local wall clock on
--                                  any server not set to UTC.
--
-- GetBalanceAtTime compares the two directly:
--
--     WHERE COALESCE(effective_date, created_at) > $snapshot_time
--
-- so on a non-UTC session the bound is offset from the transaction timeline by
-- the session's UTC offset, and the answer is silently wrong in whichever
-- direction the offset runs: east of UTC the range narrows and post-snapshot
-- transactions are dropped, west of UTC it widens and pre-snapshot
-- transactions are applied on top of the snapshot that already contains them.
-- Either way no error is raised -- just a wrong historical balance.
--
-- AT TIME ZONE 'UTC' makes the writer explicit, so both columns hold UTC wall
-- clocks and the comparison is correct at any session TimeZone. Only the one
-- assignment changes; the rest of the function is carried over unchanged.

-- +migrate Up

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
    -- Store the current time to use consistently throughout the function.
    -- AT TIME ZONE 'UTC' pins the recorded wall clock to UTC instead of
    -- whatever the database session's TimeZone happens to be, so
    -- snapshot_time lands on the same clock as blnk.transactions.created_at,
    -- which Go writes as txn.CreatedAt.UTC().
    v_current_time := (NOW() AT TIME ZONE 'UTC');
    
    -- Get the maximum ID first to know when to stop
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
            v_current_time,  -- Use the actual current time, not truncated to day
            v_current_time,  -- Set created_at to the same exact timestamp
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

-- +migrate Down

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
            v_current_time,  -- Use the actual current time, not truncated to day
            v_current_time,  -- Set created_at to the same exact timestamp
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
