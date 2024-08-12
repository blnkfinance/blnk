-- +migrate Up
ALTER TABLE blnk.balances
    ALTER COLUMN balance TYPE NUMERIC USING balance::NUMERIC,
    ALTER COLUMN credit_balance TYPE NUMERIC USING credit_balance::NUMERIC,
    ALTER COLUMN debit_balance TYPE NUMERIC USING debit_balance::NUMERIC,
    ALTER COLUMN currency_multiplier TYPE NUMERIC USING currency_multiplier::NUMERIC;

-- +migrate Down
ALTER TABLE blnk.balances
    ALTER COLUMN balance TYPE BIGINT USING balance::BIGINT,
    ALTER COLUMN credit_balance TYPE BIGINT USING credit_balance::BIGINT,
    ALTER COLUMN debit_balance TYPE BIGINT USING debit_balance::BIGINT,
    ALTER COLUMN currency_multiplier TYPE BIGINT USING currency_multiplier::BIGINT;