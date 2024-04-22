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
