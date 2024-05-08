-- +migrate Up
ALTER TABLE blnk.balance_monitors ADD COLUMN precision FLOAT;
ALTER TABLE blnk.balance_monitors ADD COLUMN precise_value BIGINT;

-- +migrate Down
ALTER TABLE blnk.balance_monitors  DROP COLUMN precision;
ALTER TABLE blnk.balance_monitors  DROP COLUMN precise_value;