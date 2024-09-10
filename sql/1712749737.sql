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
create or replace function blnk.get_balances_by_id(source_id TEXT, destination_id TEXT)
    returns table(balance_id TEXT, currency TEXT, currency_multiplier BIGINT,ledger_id TEXT, balance BIGINT, credit_balance BIGINT, debit_balance BIGINT, inflight_balance BIGINT, inflight_credit_balance BIGINT, inflight_debit_balance BIGINT, created_at TIMESTAMP, version INTEGER)
    language sql
    immutable
    strict
as
$$
select b.balance_id,b.currency,b.currency_multiplier,b.ledger_id,b.balance,b.credit_balance,b.debit_balance,b.inflight_balance,b.inflight_credit_balance,b.inflight_debit_balance,b.created_at,b.version FROM blnk.balances b WHERE b.balance_id IN (source_id, destination_id)
$$;


-- +migrate StatementBegin
create or replace FUNCTION blnk.capture_changes()
    returns TRIGGER
as
$$
declare
    payload JSON;
begin
    payload := json_build_object(
            'table', TG_TABLE_NAME,
            'data', row_to_json(NEW)
        );
    perform pg_notify('data_change', payload::text);
    return NEW;
end;
$$
language plpgsql;
-- +migrate StatementEnd

-- +migrate Up
CREATE TRIGGER transaction_after_insert AFTER INSERT ON blnk.transactions  FOR EACH ROW EXECUTE FUNCTION blnk.capture_changes();
CREATE TRIGGER ledger_after_insert AFTER INSERT ON blnk.ledgers FOR EACH ROW EXECUTE FUNCTION blnk.capture_changes();
CREATE TRIGGER balance_after_insert AFTER INSERT ON blnk.balances FOR EACH ROW EXECUTE FUNCTION blnk.capture_changes();
CREATE TRIGGER balance_after_update AFTER UPDATE ON blnk.balances FOR EACH ROW EXECUTE FUNCTION blnk.capture_changes();