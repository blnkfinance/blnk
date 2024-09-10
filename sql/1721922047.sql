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
CREATE SCHEMA IF NOT EXISTS blnk;

-- Create reconciliations table
CREATE TABLE IF NOT EXISTS blnk.reconciliations (
    id SERIAL PRIMARY KEY,
    reconciliation_id TEXT NOT NULL,
    upload_id TEXT NOT NULL,
    status VARCHAR(50) NOT NULL,
    matched_transactions INT NOT NULL,
    unmatched_transactions INT NOT NULL,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP
);

-- Create matches table
CREATE TABLE IF NOT EXISTS blnk.matches (
    id SERIAL PRIMARY KEY,
    external_transaction_id TEXT NOT NULL,
    internal_transaction_id TEXT NOT NULL,
    reconciliation_id TEXT NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    date TIMESTAMP NOT NULL
);

-- Create unmatched table
CREATE TABLE IF NOT EXISTS blnk.unmatched (
    id SERIAL PRIMARY KEY,
    external_transaction_id TEXT NOT NULL,
    reconciliation_id TEXT NOT NULL,
    date TIMESTAMP NOT NULL
);

-- Create external_transactions table
CREATE TABLE IF NOT EXISTS blnk.external_transactions (
    id TEXT PRIMARY KEY,
    amount NUMERIC(12, 2) NOT NULL,
    reference VARCHAR(255) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    description TEXT,
    date TIMESTAMP NOT NULL,
    source VARCHAR(255) NOT NULL,
    upload_id TEXT NOT NULL
);

-- Create reconciliation_progress table
CREATE TABLE IF NOT EXISTS blnk.reconciliation_progress (
    processed_count BIGINT,
    reconciliation_id TEXT UNIQUE,
    last_processed_external_txn_id TEXT
);

-- Create matching_rules table
CREATE TABLE IF NOT EXISTS blnk.matching_rules (
    id SERIAL PRIMARY KEY,
    rule_id TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    criteria JSONB NOT NULL
);

-- Create indexes
CREATE INDEX idx_reconciliations_upload_id ON blnk.reconciliations (upload_id);
CREATE INDEX idx_external_transactions_upload_id ON blnk.external_transactions (upload_id);
CREATE INDEX idx_reconciliations_progress_id ON blnk.reconciliation_progress (reconciliation_id);

-- +migrate Down
DROP INDEX IF EXISTS blnk.idx_reconciliations_upload_id;
DROP INDEX IF EXISTS blnk.idx_external_transactions_upload_id;
DROP INDEX IF EXISTS blnk.idx_reconciliations_progress_id;
DROP TABLE IF EXISTS blnk.matching_rules CASCADE;
DROP TABLE IF EXISTS blnk.external_transactions CASCADE;
DROP TABLE IF EXISTS blnk.matches CASCADE;
DROP TABLE IF EXISTS blnk.reconciliations CASCADE;
DROP TABLE IF EXISTS blnk.reconciliation_progress CASCADE;
