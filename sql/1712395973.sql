-- +migrate Up
ALTER TABLE blnk.balances ADD COLUMN inflight_balance BIGINT DEFAULT 0 NOT NULL;;
ALTER TABLE blnk.balances ADD COLUMN inflight_credit_balance BIGINT DEFAULT 0 NOT NULL;
ALTER TABLE blnk.balances ADD COLUMN inflight_debit_balance BIGINT DEFAULT 0 NOT NULL;
