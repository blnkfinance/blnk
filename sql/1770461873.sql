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

-- +migrate Up
-- Add key_prefix column for efficient lookup with bcrypt hashing
ALTER TABLE blnk.api_keys ADD COLUMN IF NOT EXISTS key_prefix VARCHAR(16);

-- Create index on key_prefix for efficient lookup
CREATE INDEX IF NOT EXISTS idx_api_keys_key_prefix ON blnk.api_keys(key_prefix);

-- +migrate Down
DROP INDEX IF EXISTS idx_api_keys_key_prefix;
ALTER TABLE blnk.api_keys DROP COLUMN IF EXISTS key_prefix;
