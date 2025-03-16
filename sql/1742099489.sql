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

-- +migrate StatementBegin
CREATE OR REPLACE FUNCTION blnk.prevent_snapshot_update()
   RETURNS TRIGGER
   LANGUAGE plpgsql
AS $$
BEGIN
   -- Allow updates to meta_data column only
   IF (NEW.meta_data IS DISTINCT FROM OLD.meta_data) AND (
       NEW.balance_id = OLD.balance_id AND
       NEW.ledger_id = OLD.ledger_id AND
       NEW.balance = OLD.balance AND
       NEW.credit_balance = OLD.credit_balance AND
       NEW.debit_balance = OLD.debit_balance AND
       NEW.inflight_balance = OLD.inflight_balance AND
       NEW.inflight_credit_balance = OLD.inflight_credit_balance AND
       NEW.inflight_debit_balance = OLD.inflight_debit_balance AND
       NEW.currency = OLD.currency AND
       NEW.snapshot_time = OLD.snapshot_time AND
       NEW.created_at = OLD.created_at AND
       NEW.last_tx_id = OLD.last_tx_id
   ) THEN
       RETURN NEW;
   ELSE
       RAISE EXCEPTION 'Cannot modify balance snapshot data. Only meta_data field can be updated.';
   END IF;
END;
$$;
-- +migrate StatementEnd

-- +migrate StatementBegin
-- Drop the trigger if it exists and recreate it
DROP TRIGGER IF EXISTS prevent_snapshot_update_trigger ON blnk.balance_snapshots;

CREATE TRIGGER prevent_snapshot_update_trigger
   BEFORE UPDATE ON blnk.balance_snapshots
   FOR EACH ROW
   EXECUTE FUNCTION blnk.prevent_snapshot_update();
-- +migrate StatementEnd

-- +migrate Down

-- +migrate StatementBegin
DROP TRIGGER IF EXISTS prevent_snapshot_update_trigger ON blnk.balance_snapshots;
DROP FUNCTION IF EXISTS blnk.prevent_snapshot_update();
-- +migrate StatementEnd