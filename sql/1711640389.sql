-- +migrate Up
ALTER TABLE blnk.balances ADD COLUMN version INTEGER DEFAULT 1 NOT NULL;

-- +migrate Down
ALTER TABLE blnk.balances DROP COLUMN IF EXISTS version;
