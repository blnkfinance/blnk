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
ALTER TABLE blnk.balance_monitors ADD COLUMN precision FLOAT;
ALTER TABLE blnk.balance_monitors ADD COLUMN precise_value BIGINT;

-- +migrate Down
ALTER TABLE blnk.balance_monitors  DROP COLUMN precision;
ALTER TABLE blnk.balance_monitors  DROP COLUMN precise_value;