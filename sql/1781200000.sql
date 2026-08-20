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

-- balance_snapshots.snapshot_time was the only TIMESTAMPTZ column in the
-- ledger's time arithmetic, and GetBalanceAtTime uses it as the lower bound
-- of a range over blnk.transactions.created_at / effective_date, which are
-- both plain TIMESTAMP:
--
--     WHERE COALESCE(effective_date, created_at) > $snapshot_time
--
-- Postgres resolves that comparison by converting the TIMESTAMPTZ bound into
-- the session time zone. On a UTC server the offset is zero and the bound is
-- correct; on any other server it shifts by the UTC offset, widening the
-- range so that transactions from *before* the snapshot are applied on top of
-- it. The snapshot is then double-counted and GetBalanceAtTime silently
-- returns a wrong historical balance -- no error, just bad numbers.
--
-- The mismatch was almost certainly unintentional: snapshot_time and the
-- created_at directly beneath it in the original CREATE TABLE were declared
-- with different types. Plain TIMESTAMP is also the schema's overwhelming
-- convention -- 28 columns against 5 -- and the remaining four TIMESTAMPTZ
-- columns all belong to blnk.api_keys, which is internally consistent and
-- never compared against transaction timestamps. So snapshot_time is aligned
-- down to the columns it is actually compared with, rather than converting
-- the transaction history up.
--
-- USING snapshot_time AT TIME ZONE 'UTC' reads each stored instant as UTC
-- wall-clock, matching how the writer records it: take_daily_balance_snapshots
-- and the Go caller both pass values derived from time.Now(), and the shipped
-- docker-compose pins TZ=Etc/UTC. Existing rows therefore keep the instant
-- they already represent.

-- +migrate Up
ALTER TABLE blnk.balance_snapshots
    ALTER COLUMN snapshot_time TYPE TIMESTAMP
    USING snapshot_time AT TIME ZONE 'UTC';

-- +migrate Down
ALTER TABLE blnk.balance_snapshots
    ALTER COLUMN snapshot_time TYPE TIMESTAMP WITH TIME ZONE
    USING snapshot_time AT TIME ZONE 'UTC';
