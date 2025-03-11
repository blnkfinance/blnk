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
CREATE TABLE IF NOT EXISTS blnk.api_keys (
   id         SERIAL PRIMARY KEY,
   api_key_id TEXT NOT NULL UNIQUE,
   key        TEXT NOT NULL UNIQUE,
   name       TEXT NOT NULL,
   owner_id   TEXT NOT NULL,
   scopes     TEXT[] NOT NULL DEFAULT '{}',
   expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
   created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
   last_used_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
   is_revoked BOOLEAN NOT NULL DEFAULT FALSE,
   revoked_at TIMESTAMP WITH TIME ZONE
);

-- +migrate Up
CREATE INDEX IF NOT EXISTS idx_api_keys_owner_id ON blnk.api_keys(owner_id);
CREATE INDEX IF NOT EXISTS idx_api_keys_key ON blnk.api_keys(key);

-- +migrate Down
DROP TABLE IF EXISTS blnk.api_keys;
DROP INDEX IF EXISTS idx_api_keys_owner_id;
DROP INDEX IF EXISTS idx_api_keys_key;