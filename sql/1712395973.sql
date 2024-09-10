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
ALTER TABLE blnk.balances ADD COLUMN inflight_balance BIGINT DEFAULT 0 NOT NULL;;
ALTER TABLE blnk.balances ADD COLUMN inflight_credit_balance BIGINT DEFAULT 0 NOT NULL;
ALTER TABLE blnk.balances ADD COLUMN inflight_debit_balance BIGINT DEFAULT 0 NOT NULL;

-- +migrate Down
ALTER TABLE blnk.balances DROP COLUMN IF EXISTS inflight_balance;
ALTER TABLE blnk.balances DROP COLUMN IF EXISTS inflight_credit_balance;
ALTER TABLE blnk.balances DROP COLUMN IF EXISTS inflight_debit_balance;
