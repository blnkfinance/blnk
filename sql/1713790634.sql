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
ALTER TABLE blnk.balance_monitors DROP CONSTRAINT IF EXISTS balance_monitors_field_check;
ALTER TABLE blnk.balance_monitors ADD CONSTRAINT balance_monitors_field_check CHECK (field IN ('debit_balance', 'credit_balance', 'balance', 'inflight_debit_balance', 'inflight_credit_balance', 'inflight_balance'));
ALTER TABLE blnk.balance_monitors DROP CONSTRAINT IF EXISTS balance_monitors_operator_check;
ALTER TABLE blnk.balance_monitors ADD CONSTRAINT balance_monitors_operator_check CHECK (operator IN ('>', '<', '>=', '<=', '=', '!='));

-- +migrate Down
ALTER TABLE blnk.balance_monitors DROP CONSTRAINT IF EXISTS balance_monitors_field_check;
ALTER TABLE blnk.balance_monitors ADD CONSTRAINT balance_monitors_field_check CHECK (field IN ('debit_balance', 'credit_balance', 'balance'));
ALTER TABLE blnk.balance_monitors DROP CONSTRAINT IF EXISTS balance_monitors_operator_check;
ALTER TABLE blnk.balance_monitors ADD CONSTRAINT balance_monitors_operator_check CHECK (operator IN ('>', '<', '>=', '<=', '='));
