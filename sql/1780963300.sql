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

-- Drop unused columns: the per-transaction FX `rate`, and `currency_multiplier`
-- and `modification_ref` on balances. None was read by any computation; `rate`
-- additionally broke inflight commit (rated on hold, un-rated on commit), and
-- `modification_ref` was never referenced by any code or SQL object. The
-- get_balances_by_id function references currency_multiplier, so it is
-- recreated without the column first.

-- +migrate Up
DROP FUNCTION IF EXISTS blnk.get_balances_by_id(TEXT, TEXT);

-- +migrate StatementBegin
CREATE OR REPLACE FUNCTION blnk.get_balances_by_id(source_id TEXT, destination_id TEXT)
    RETURNS TABLE(balance_id TEXT, currency TEXT, ledger_id TEXT, balance BIGINT, credit_balance BIGINT, debit_balance BIGINT, inflight_balance BIGINT, inflight_credit_balance BIGINT, inflight_debit_balance BIGINT, created_at TIMESTAMP, version INTEGER)
    LANGUAGE sql
    IMMUTABLE
    STRICT
AS
$$
SELECT b.balance_id, b.currency, b.ledger_id, b.balance, b.credit_balance, b.debit_balance, b.inflight_balance, b.inflight_credit_balance, b.inflight_debit_balance, b.created_at, b.version FROM blnk.balances b WHERE b.balance_id IN (source_id, destination_id)
$$;
-- +migrate StatementEnd

-- +migrate Up
ALTER TABLE blnk.transactions DROP COLUMN IF EXISTS rate;
ALTER TABLE blnk.balances DROP COLUMN IF EXISTS currency_multiplier;
ALTER TABLE blnk.balances DROP COLUMN IF EXISTS modification_ref;

-- +migrate Down
ALTER TABLE blnk.balances ADD COLUMN IF NOT EXISTS modification_ref TEXT;
ALTER TABLE blnk.balances ADD COLUMN IF NOT EXISTS currency_multiplier NUMERIC NOT NULL DEFAULT 1;
ALTER TABLE blnk.transactions ADD COLUMN IF NOT EXISTS rate FLOAT DEFAULT 1;

DROP FUNCTION IF EXISTS blnk.get_balances_by_id(TEXT, TEXT);

-- +migrate StatementBegin
CREATE OR REPLACE FUNCTION blnk.get_balances_by_id(source_id TEXT, destination_id TEXT)
    RETURNS TABLE(balance_id TEXT, currency TEXT, currency_multiplier BIGINT, ledger_id TEXT, balance BIGINT, credit_balance BIGINT, debit_balance BIGINT, inflight_balance BIGINT, inflight_credit_balance BIGINT, inflight_debit_balance BIGINT, created_at TIMESTAMP, version INTEGER)
    LANGUAGE sql
    IMMUTABLE
    STRICT
AS
$$
SELECT b.balance_id, b.currency, b.currency_multiplier, b.ledger_id, b.balance, b.credit_balance, b.debit_balance, b.inflight_balance, b.inflight_credit_balance, b.inflight_debit_balance, b.created_at, b.version FROM blnk.balances b WHERE b.balance_id IN (source_id, destination_id)
$$;
-- +migrate StatementEnd
