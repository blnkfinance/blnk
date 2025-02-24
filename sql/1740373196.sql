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
CREATE TABLE IF NOT EXISTS blnk.balance_snapshots
(
    id                      SERIAL PRIMARY KEY,
    balance_id              TEXT      NOT NULL REFERENCES blnk.balances (balance_id),
    ledger_id              TEXT      NOT NULL REFERENCES blnk.ledgers (ledger_id),
    balance                 BIGINT    NOT NULL,
    credit_balance         BIGINT    NOT NULL,
    debit_balance          BIGINT    NOT NULL,
    inflight_balance       BIGINT    NOT NULL,
    inflight_credit_balance BIGINT    NOT NULL,
    inflight_debit_balance  BIGINT    NOT NULL,
    currency               TEXT      NOT NULL,
    snapshot_time          TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at             TIMESTAMP NOT NULL,
    last_tx_id             TEXT,
    meta_data              JSONB
);

-- +migrate Up
CREATE INDEX idx_balance_snapshots_balance_id ON blnk.balance_snapshots(balance_id);
CREATE INDEX idx_balance_snapshots_snapshot_time ON blnk.balance_snapshots(snapshot_time);
CREATE INDEX idx_balance_snapshots_ledger_time ON blnk.balance_snapshots(ledger_id, snapshot_time);

-- +migrate Down
DROP INDEX IF EXISTS blnk.idx_balance_snapshots_ledger_time;
DROP INDEX IF EXISTS blnk.idx_balance_snapshots_snapshot_time;
DROP INDEX IF EXISTS blnk.idx_balance_snapshots_balance_id;
DROP TABLE IF EXISTS blnk.balance_snapshots;