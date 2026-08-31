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
-- the session time zone. created_at is written by Go as txn.CreatedAt.UTC(),
-- so it is a UTC wall clock on every host. On a UTC session the conversion is
-- a no-op and the bound lines up; on any other session it moves by the UTC
-- offset and GetBalanceAtTime silently returns a wrong historical balance --
-- no error, just bad numbers, in whichever direction the offset runs. East of
-- UTC the range narrows and post-snapshot transactions are dropped; west of
-- UTC it widens and pre-snapshot transactions are applied on top of the
-- snapshot that already accounts for them.
--
-- This migration removes the reinterpretation at read time. It is half the
-- fix: it does not say which clock the writer records, and the writer records
-- the session's. The companion migration 1781200001 pins that side to UTC.
-- Neither alone is correct on a non-UTC session; together they make the
-- endpoint independent of the session's TimeZone. Covered by
-- TestGetBalanceAtTime_NonUTCSession_RealDB.
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
-- USING snapshot_time AT TIME ZONE 'UTC' reads each stored instant as its UTC
-- wall clock, which is the clock blnk.transactions.created_at is already on.
-- Existing rows therefore keep the instant they already represent, and land on
-- the same timeline as the transactions they are compared against.

-- +migrate Up
ALTER TABLE blnk.balance_snapshots
    ALTER COLUMN snapshot_time TYPE TIMESTAMP
    USING snapshot_time AT TIME ZONE 'UTC';

-- +migrate Down
ALTER TABLE blnk.balance_snapshots
    ALTER COLUMN snapshot_time TYPE TIMESTAMP WITH TIME ZONE
    USING snapshot_time AT TIME ZONE 'UTC';
