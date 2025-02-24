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
BEGIN
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
                AND DATE_TRUNC('day', bs.snapshot_time) = DATE_TRUNC('day', NOW())
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
            DATE_TRUNC('day', NOW()),
            b.created_at,
            COALESCE((
                SELECT t.transaction_id 
                FROM blnk.transactions t
                WHERE t.source = b.balance_id OR t.destination = b.balance_id
                ORDER BY t.created_at DESC 
                LIMIT 1
            ), NULL),
            b.meta_data
        FROM batch b;

        GET DIAGNOSTICS v_batch_count = ROW_COUNT;
        v_total_processed := v_total_processed + v_batch_count;
        
        -- Get the last ID processed in this batch
        SELECT COALESCE(MAX(id), v_max_id) INTO v_last_processed_id
        FROM blnk.balances
        WHERE id > v_last_processed_id
        ORDER BY id
        LIMIT batch_size;

        -- Exit if we've processed everything or no more records to process
        EXIT WHEN v_batch_count = 0 OR v_last_processed_id >= v_max_id;
    END LOOP;

    RETURN v_total_processed;
END;
$$;
-- +migrate StatementEnd

-- +migrate Down

-- +migrate StatementBegin
DROP FUNCTION IF EXISTS blnk.take_daily_balance_snapshots_batched;
-- +migrate StatementEnd