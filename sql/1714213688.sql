-- +migrate Up
ALTER TABLE blnk.transactions ADD COLUMN parent_transaction TEXT;

-- +migrate Down
ALTER TABLE blnk.transactions  DROP COLUMN parent_transaction;