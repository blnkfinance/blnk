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