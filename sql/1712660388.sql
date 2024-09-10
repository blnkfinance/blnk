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
ALTER TABLE blnk.transactions ALTER COLUMN amount TYPE FLOAT USING amount::FLOAT;
ALTER TABLE blnk.transactions ADD COLUMN precise_amount BIGINT;
ALTER TABLE blnk.transactions ADD COLUMN precision BIGINT DEFAULT 1;
ALTER TABLE blnk.transactions ADD COLUMN rate BIGINT DEFAULT 1;


-- +migrate Up
DROP INDEX IF EXISTS blnk.idx_unique_indicator_on_non_nulls;


-- +migrate Down
ALTER TABLE blnk.transactions ALTER COLUMN amount TYPE BIGINT USING amount::BIGINT;
ALTER TABLE blnk.transactions  DROP COLUMN precise_amount;
ALTER TABLE blnk.transactions  DROP COLUMN precision;
ALTER TABLE blnk.transactions  DROP COLUMN rate;


