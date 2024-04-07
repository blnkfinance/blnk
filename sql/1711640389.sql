-- +migrate Up
ALTER TABLE blnk.balances ADD COLUMN version INTEGER DEFAULT 1 NOT NULL;
