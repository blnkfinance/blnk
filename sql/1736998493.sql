-- +migrate Up
ALTER TABLE blnk.transactions ALTER COLUMN rate TYPE FLOAT;

-- +migrate Down
ALTER TABLE blnk.transactions ALTER COLUMN rate TYPE BIGINT;
