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

-- Drops the data_change NOTIFY triggers. Nothing listens on that channel.
--
-- Dropping a trigger takes a brief exclusive lock on its table. lock_timeout
-- makes the migration fail instead of blocking traffic behind a long-running
-- query; rerun it once that query finishes. Balances go first because writers
-- lock balances before inserting the transaction.

-- +migrate Up
SET LOCAL lock_timeout = '3s';
DROP TRIGGER IF EXISTS balance_after_update ON blnk.balances;
DROP TRIGGER IF EXISTS balance_after_insert ON blnk.balances;
DROP TRIGGER IF EXISTS ledger_after_insert ON blnk.ledgers;
DROP TRIGGER IF EXISTS transaction_after_insert ON blnk.transactions;
DROP FUNCTION IF EXISTS blnk.capture_changes();

-- +migrate Down
SET LOCAL lock_timeout = '3s';

-- +migrate StatementBegin
CREATE OR REPLACE FUNCTION blnk.capture_changes()
    RETURNS TRIGGER
AS
$$
DECLARE
    payload JSON;
BEGIN
    payload := json_build_object(
            'table', TG_TABLE_NAME,
            'data', row_to_json(NEW)
        );
    PERFORM pg_notify('data_change', payload::text);
    RETURN NEW;
END;
$$
LANGUAGE plpgsql;
-- +migrate StatementEnd

CREATE TRIGGER balance_after_update AFTER UPDATE ON blnk.balances FOR EACH ROW EXECUTE FUNCTION blnk.capture_changes();
CREATE TRIGGER balance_after_insert AFTER INSERT ON blnk.balances FOR EACH ROW EXECUTE FUNCTION blnk.capture_changes();
CREATE TRIGGER ledger_after_insert AFTER INSERT ON blnk.ledgers FOR EACH ROW EXECUTE FUNCTION blnk.capture_changes();
CREATE TRIGGER transaction_after_insert AFTER INSERT ON blnk.transactions FOR EACH ROW EXECUTE FUNCTION blnk.capture_changes();
