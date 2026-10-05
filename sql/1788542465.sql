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

-- AlertCondition.Value is a float64 and is the human-readable threshold that
-- pairs with precision, but the column was a BIGINT: a monitor on "balance <
-- 100.50" failed at the database and surfaced as a 500. precise_value carries
-- the value the ledger actually compares against, so widening this column
-- changes no evaluation, it only stops rejecting thresholds the API accepts.

-- +migrate Up
ALTER TABLE blnk.balance_monitors ALTER COLUMN value TYPE DOUBLE PRECISION;

-- +migrate Down
ALTER TABLE blnk.balance_monitors ALTER COLUMN value TYPE BIGINT USING ROUND(value);
