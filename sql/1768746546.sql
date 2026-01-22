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

-- Add fund lineage tracking columns to balances table
ALTER TABLE blnk.balances ADD COLUMN IF NOT EXISTS track_fund_lineage BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE blnk.balances ADD COLUMN IF NOT EXISTS allocation_strategy TEXT NOT NULL DEFAULT 'FIFO';

-- Create lineage_mappings table to track relationships between main balances and shadow balances
CREATE TABLE IF NOT EXISTS blnk.lineage_mappings (
    id SERIAL PRIMARY KEY,
    balance_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    shadow_balance_id TEXT NOT NULL,
    aggregate_balance_id TEXT NOT NULL,
    identity_id TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(balance_id, provider)
);

-- Create indexes for efficient lookups
CREATE INDEX IF NOT EXISTS idx_lineage_mappings_balance_id ON blnk.lineage_mappings(balance_id);
CREATE INDEX IF NOT EXISTS idx_lineage_mappings_shadow_balance_id ON blnk.lineage_mappings(shadow_balance_id);
CREATE INDEX IF NOT EXISTS idx_lineage_mappings_provider ON blnk.lineage_mappings(provider);

-- +migrate Down

-- Drop indexes
DROP INDEX IF EXISTS blnk.idx_lineage_mappings_balance_id;
DROP INDEX IF EXISTS blnk.idx_lineage_mappings_shadow_balance_id;
DROP INDEX IF EXISTS blnk.idx_lineage_mappings_provider;

-- Drop lineage_mappings table
DROP TABLE IF EXISTS blnk.lineage_mappings;

-- Remove columns from balances table
ALTER TABLE blnk.balances DROP COLUMN IF EXISTS track_fund_lineage;
ALTER TABLE blnk.balances DROP COLUMN IF EXISTS allocation_strategy;
