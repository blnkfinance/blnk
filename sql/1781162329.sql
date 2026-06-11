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

-- The chainer's scan index covers all rows until they are sealed, so build it
-- CONCURRENTLY (requires notransaction) to avoid locking the transactions table
-- on large installs. In steady state it holds only the last few seconds of
-- rows, so the poll query never degrades as the table grows.

-- +migrate Up notransaction
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transactions_unchained
    ON blnk.transactions (id) WHERE chain_seq IS NULL;

-- +migrate Down notransaction
DROP INDEX CONCURRENTLY IF EXISTS blnk.idx_transactions_unchained;
