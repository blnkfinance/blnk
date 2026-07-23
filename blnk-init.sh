#!/bin/sh
# Copyright 2024 Blnk Finance Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Creates blnk.json with docker-compose defaults when missing. Edit blnk.json
# directly for custom configuration.

set -eu

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG_FILE="${ROOT_DIR}/blnk.json"

if [ -f "${CONFIG_FILE}" ]; then
    exit 0
fi

# Docker creates a directory when the bind-mount source file is missing.
if [ -d "${CONFIG_FILE}" ]; then
    rm -rf "${CONFIG_FILE}"
fi

cat > "${CONFIG_FILE}" <<'EOF'
{
  "project_name": "Blnk",
  "data_source": {
    "dns": "postgres://postgres:password@postgres:5432/blnk?sslmode=disable"
  },
  "redis": {
    "dns": "redis:6379"
  },
  "server": {
    "port": "5001"
  }
}
EOF

echo "Created ${CONFIG_FILE} with compose defaults."
echo "Edit ${CONFIG_FILE} to customize your Blnk configuration."
