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
CREATE OR REPLACE FUNCTION blnk.prevent_transaction_update()
    RETURNS TRIGGER
    LANGUAGE plpgsql
AS $$
BEGIN
    -- Allow updates to meta_data column regardless of whether it's changed or not
    IF (
        NEW.transaction_id = OLD.transaction_id AND
        NEW.source = OLD.source AND
        NEW.destination = OLD.destination AND
        NEW.description = OLD.description AND
        NEW.reference = OLD.reference AND
        NEW.amount = OLD.amount AND
        NEW.currency = OLD.currency AND
        NEW.status = OLD.status AND
        NEW.hash = OLD.hash AND
        NEW.created_at = OLD.created_at AND
        NEW.scheduled_for = OLD.scheduled_for
    ) THEN
        -- Only meta_data is potentially different, which is allowed
        RETURN NEW;
    ELSE
        RAISE EXCEPTION 'Cannot modify transaction data. Only meta_data field can be updated.';
    END IF;
END;
$$;
-- +migrate StatementEnd

-- +migrate StatementBegin
-- Drop the trigger if it exists and recreate it
DROP TRIGGER IF EXISTS prevent_transaction_update_trigger ON blnk.transactions;

CREATE TRIGGER prevent_transaction_update_trigger
    BEFORE UPDATE ON blnk.transactions
    FOR EACH ROW
    EXECUTE FUNCTION blnk.prevent_transaction_update();
-- +migrate StatementEnd

-- +migrate Down

-- +migrate StatementBegin
DROP TRIGGER IF EXISTS prevent_transaction_update_trigger ON blnk.transactions;
DROP FUNCTION IF EXISTS blnk.prevent_transaction_update();
-- +migrate StatementEnd