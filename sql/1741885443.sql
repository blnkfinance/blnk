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
ALTER TABLE blnk.transactions ALTER COLUMN amount TYPE NUMERIC USING amount::NUMERIC;
ALTER TABLE blnk.transactions ALTER COLUMN precise_amount TYPE NUMERIC USING precise_amount::NUMERIC;

ALTER TABLE blnk.balances ALTER COLUMN balance TYPE NUMERIC;
ALTER TABLE blnk.balances ALTER COLUMN credit_balance TYPE NUMERIC;
ALTER TABLE blnk.balances ALTER COLUMN debit_balance TYPE NUMERIC;

ALTER TABLE blnk.balances ALTER COLUMN inflight_balance TYPE NUMERIC;
ALTER TABLE blnk.balances ALTER COLUMN inflight_credit_balance TYPE NUMERIC;
ALTER TABLE blnk.balances ALTER COLUMN inflight_debit_balance TYPE NUMERIC;

-- +migrate Down
ALTER TABLE blnk.transactions ALTER COLUMN amount TYPE FLOAT USING amount::FLOAT;
ALTER TABLE blnk.transactions ALTER COLUMN precise_amount TYPE BIGINT USING precise_amount::BIGINT;

ALTER TABLE blnk.balances ALTER COLUMN balance TYPE BIGINT USING balance::BIGINT;
ALTER TABLE blnk.balances ALTER COLUMN credit_balance TYPE BIGINT USING credit_balance::BIGINT;
ALTER TABLE blnk.balances ALTER COLUMN debit_balance TYPE BIGINT USING debit_balance::BIGINT;

ALTER TABLE blnk.balances ALTER COLUMN inflight_balance TYPE BIGINT USING inflight_balance::BIGINT;
ALTER TABLE blnk.balances ALTER COLUMN inflight_credit_balance TYPE BIGINT USING inflight_credit_balance::BIGINT;
ALTER TABLE blnk.balances ALTER COLUMN inflight_debit_balance TYPE BIGINT USING inflight_debit_balance::BIGINT;