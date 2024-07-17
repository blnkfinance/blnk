-- +migrate Up
CREATE INDEX IF NOT EXISTS idx_transactions_status ON blnk.transactions (status);
CREATE INDEX IF NOT EXISTS idx_transactions_parent_transaction ON blnk.transactions (parent_transaction);

-- +migrate Down
DROP INDEX IF EXISTS blnk.idx_transactions_status;
DROP INDEX IF EXISTS blnk.idx_transactions_parent_transaction;
