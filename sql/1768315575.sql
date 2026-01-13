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

-- Transactions table indexes
CREATE INDEX IF NOT EXISTS blnk_transactions_currency_created_at_idx ON blnk.transactions (currency, created_at DESC);
CREATE INDEX IF NOT EXISTS blnk_transactions_created_at_source_idx ON blnk.transactions (created_at, source);
CREATE INDEX IF NOT EXISTS blnk_transactions_created_at_destination_idx ON blnk.transactions (created_at, destination);
CREATE INDEX IF NOT EXISTS blnk_txn_created_at_not_queued_idx ON blnk.transactions (created_at DESC) WHERE status <> 'QUEUED';
CREATE INDEX IF NOT EXISTS blnk_txn_currency_created_at_desc_idx ON blnk.transactions (currency, created_at DESC) INCLUDE (precision);
CREATE INDEX IF NOT EXISTS idx_transactions_status ON blnk.transactions (status);
CREATE INDEX IF NOT EXISTS idx_transactions_currency ON blnk.transactions (currency);
CREATE INDEX IF NOT EXISTS idx_transactions_source ON blnk.transactions (source);
CREATE INDEX IF NOT EXISTS idx_transactions_destination ON blnk.transactions (destination);
CREATE INDEX IF NOT EXISTS idx_transactions_parent_transaction ON blnk.transactions (parent_transaction);
CREATE INDEX IF NOT EXISTS idx_transactions_meta_data ON blnk.transactions USING gin (meta_data);

-- Balances table indexes
CREATE INDEX IF NOT EXISTS blnk_balances_created_at_idx ON blnk.balances (created_at DESC);
CREATE INDEX IF NOT EXISTS blnk_balances_identity_id_idx ON blnk.balances (identity_id);
CREATE INDEX IF NOT EXISTS blnk_balances_ledger_id_idx ON blnk.balances (ledger_id);
CREATE INDEX IF NOT EXISTS blnk_balances_currency_identity_id_idx ON blnk.balances (currency, identity_id);

-- Identity table indexes
CREATE INDEX IF NOT EXISTS blnk_identity_created_at_idx ON blnk.identity (created_at DESC);
CREATE INDEX IF NOT EXISTS blnk_identity_country_created_at_idx ON blnk.identity (country, created_at DESC);

-- Ledgers table indexes
CREATE INDEX IF NOT EXISTS blnk_ledgers_created_at_idx ON blnk.ledgers (created_at DESC);

-- +migrate Down

-- Drop transactions table indexes
DROP INDEX IF EXISTS blnk.blnk_transactions_currency_created_at_idx;
DROP INDEX IF EXISTS blnk.blnk_transactions_created_at_source_idx;
DROP INDEX IF EXISTS blnk.blnk_transactions_created_at_destination_idx;
DROP INDEX IF EXISTS blnk.blnk_txn_created_at_not_queued_idx;
DROP INDEX IF EXISTS blnk.blnk_txn_currency_created_at_desc_idx;
DROP INDEX IF EXISTS blnk.idx_transactions_currency;
DROP INDEX IF EXISTS blnk.idx_transactions_source;
DROP INDEX IF EXISTS blnk.idx_transactions_destination;
DROP INDEX IF EXISTS blnk.idx_transactions_meta_data;

-- Drop balances table indexes
DROP INDEX IF EXISTS blnk.blnk_balances_created_at_idx;
DROP INDEX IF EXISTS blnk.blnk_balances_identity_id_idx;
DROP INDEX IF EXISTS blnk.blnk_balances_ledger_id_idx;
DROP INDEX IF EXISTS blnk.blnk_balances_currency_identity_id_idx;

-- Drop identity table indexes
DROP INDEX IF EXISTS blnk.blnk_identity_created_at_idx;
DROP INDEX IF EXISTS blnk.blnk_identity_country_created_at_idx;

-- Drop ledgers table indexes
DROP INDEX IF EXISTS blnk.blnk_ledgers_created_at_idx;

