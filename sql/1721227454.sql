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
CREATE INDEX IF NOT EXISTS idx_transactions_status ON blnk.transactions (status);
CREATE INDEX IF NOT EXISTS idx_transactions_parent_transaction ON blnk.transactions (parent_transaction);

-- +migrate Down
DROP INDEX IF EXISTS blnk.idx_transactions_status;
DROP INDEX IF EXISTS blnk.idx_transactions_parent_transaction;
