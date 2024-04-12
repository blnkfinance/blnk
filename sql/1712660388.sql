-- +migrate Up
ALTER TABLE blnk.transactions ALTER COLUMN amount TYPE FLOAT USING amount::FLOAT;
ALTER TABLE blnk.transactions ADD COLUMN precise_amount BIGINT;
ALTER TABLE blnk.transactions ADD COLUMN precision BIGINT DEFAULT 1;
ALTER TABLE blnk.transactions ADD COLUMN rate BIGINT DEFAULT 1;


-- +migrate Up
DROP INDEX IF EXISTS blnk.idx_unique_indicator_on_non_nulls;


-- +migrate Down
ALTER TABLE blnk.transactions ALTER COLUMN amount TYPE BIGINT USING amount::BIGINT;
ALTER TABLE blnk.transactions  DROP COLUMN precise_amount;
ALTER TABLE blnk.transactions  DROP COLUMN precision;
ALTER TABLE blnk.transactions  DROP COLUMN rate;


