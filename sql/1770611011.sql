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

-- Unique index on transaction reference to enforce idempotency
CREATE UNIQUE INDEX IF NOT EXISTS idx_transactions_reference_unique
ON blnk.transactions (reference);

-- Partial index for stuck QUEUED transaction recovery
-- Covers the recovery query: WHERE status = 'QUEUED' AND created_at < threshold
-- The parent_transaction index (idx_transactions_parent_transaction) already exists for the NOT EXISTS subquery
CREATE INDEX IF NOT EXISTS idx_transactions_queued_recovery
ON blnk.transactions (created_at ASC)
WHERE status = 'QUEUED';

-- +migrate Down

DROP INDEX IF EXISTS blnk.idx_transactions_queued_recovery;
DROP INDEX IF EXISTS blnk.idx_transactions_reference_unique;
