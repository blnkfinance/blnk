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

-- Edge-triggered balance monitors (#318). condition_state is the last observed
-- truth value of the monitor's condition; state_version is the balance version
-- that produced it, which lets a concurrent evaluation running on an older
-- balance be discarded instead of flipping the state back.
--
-- Existing monitors adopt 'edge' and start armed, so a monitor whose condition
-- already holds fires once on its balance's next transaction and then goes quiet.

-- +migrate Up
ALTER TABLE blnk.balance_monitors ADD COLUMN trigger_type TEXT NOT NULL DEFAULT 'edge';
ALTER TABLE blnk.balance_monitors ADD CONSTRAINT balance_monitors_trigger_type_check CHECK (trigger_type IN ('edge', 'level'));
ALTER TABLE blnk.balance_monitors ADD COLUMN condition_state BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE blnk.balance_monitors ADD COLUMN state_version BIGINT NOT NULL DEFAULT 0;
ALTER TABLE blnk.balance_monitors ADD COLUMN state_changed_at TIMESTAMP;

-- +migrate Down
ALTER TABLE blnk.balance_monitors DROP CONSTRAINT IF EXISTS balance_monitors_trigger_type_check;
ALTER TABLE blnk.balance_monitors DROP COLUMN state_changed_at;
ALTER TABLE blnk.balance_monitors DROP COLUMN state_version;
ALTER TABLE blnk.balance_monitors DROP COLUMN condition_state;
ALTER TABLE blnk.balance_monitors DROP COLUMN trigger_type;
