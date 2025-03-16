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
-- Create the transaction journal table
CREATE TABLE IF NOT EXISTS blnk.transaction_journal (
    id SERIAL PRIMARY KEY,
    transaction_id TEXT NOT NULL,
    action_type TEXT NOT NULL CHECK (action_type IN ('INSERT', 'UPDATE', 'DELETE')),
    client_addr INET,
    backend_pid INTEGER,
    timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    old_data JSONB,
    new_data JSONB,
    succeeded BOOLEAN NOT NULL
);

-- Create the logging function
CREATE OR REPLACE FUNCTION blnk.log_transaction_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'UPDATE') THEN
        INSERT INTO blnk.transaction_journal (
            transaction_id, action_type, client_addr, backend_pid, timestamp, 
            old_data, new_data, succeeded
        ) VALUES (
            OLD.transaction_id, 'UPDATE', 
            CASE WHEN pg_catalog.inet_client_addr() IS NULL 
                THEN NULL 
                ELSE inet(pg_catalog.inet_client_addr()) 
            END,
            pg_backend_pid(),
            NOW(),
            row_to_json(OLD)::jsonb, row_to_json(NEW)::jsonb, TRUE
        );
    ELSIF (TG_OP = 'INSERT') THEN
        INSERT INTO blnk.transaction_journal (
            transaction_id, action_type, client_addr, backend_pid, timestamp, 
            old_data, new_data, succeeded
        ) VALUES (
            NEW.transaction_id, 'INSERT', 
            CASE WHEN pg_catalog.inet_client_addr() IS NULL 
                THEN NULL 
                ELSE inet(pg_catalog.inet_client_addr()) 
            END,
            pg_backend_pid(),
            NOW(),
            NULL, row_to_json(NEW)::jsonb, TRUE
        );
    ELSIF (TG_OP = 'DELETE') THEN
        INSERT INTO blnk.transaction_journal (
            transaction_id, action_type, client_addr, backend_pid, timestamp, 
            old_data, new_data, succeeded
        ) VALUES (
            OLD.transaction_id, 'DELETE', 
            CASE WHEN pg_catalog.inet_client_addr() IS NULL 
                THEN NULL 
                ELSE inet(pg_catalog.inet_client_addr()) 
            END,
            pg_backend_pid(),
            NOW(),
            row_to_json(OLD)::jsonb, NULL, TRUE
        );
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
-- +migrate StatementEnd

-- +migrate StatementBegin
-- Create the trigger to use the function
DROP TRIGGER IF EXISTS log_transaction_changes_trigger ON blnk.transactions;

CREATE TRIGGER log_transaction_changes_trigger
AFTER INSERT OR UPDATE OR DELETE ON blnk.transactions
FOR EACH ROW EXECUTE FUNCTION blnk.log_transaction_changes();
-- +migrate StatementEnd

-- +migrate Down

-- +migrate StatementBegin
-- Remove the trigger
DROP TRIGGER IF EXISTS log_transaction_changes_trigger ON blnk.transactions;
-- +migrate StatementEnd

-- +migrate StatementBegin
-- Remove the function
DROP FUNCTION IF EXISTS blnk.log_transaction_changes();
-- +migrate StatementEnd

-- +migrate StatementBegin
-- Remove the journal table
DROP TABLE IF EXISTS blnk.transaction_journal;
-- +migrate StatementEnd