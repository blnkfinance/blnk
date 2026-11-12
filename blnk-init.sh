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

# Keep URL parsing deterministic and encode credentials byte-by-byte.
LC_ALL=C

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG_FILE="${ROOT_DIR}/blnk.json"

if [ -f "${CONFIG_FILE}" ]; then
    exit 0
fi

# Docker creates a directory when the bind-mount source file is missing.
if [ -d "${CONFIG_FILE}" ]; then
    rm -rf "${CONFIG_FILE}"
fi

url_encode() {
    value=$1
    encoded=""

    while [ -n "${value}" ]; do
        character=${value%"${value#?}"}
        case "${character}" in
            [a-zA-Z0-9.~_-]) encoded="${encoded}${character}" ;;
            *) encoded="${encoded}$(printf '%%%02X' "'${character}")" ;;
        esac
        value=${value#?}
    done

    printf '%s' "${encoded}"
}

json_escape() {
    printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

POSTGRES_USER=${POSTGRES_USER:-postgres}
POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-password}
POSTGRES_DB=${POSTGRES_DB:-blnk}

if [ -n "${BLNK_DATA_SOURCE_DNS:-}" ]; then
    DATA_SOURCE_DNS=${BLNK_DATA_SOURCE_DNS}
else
    ENCODED_USER=$(url_encode "${POSTGRES_USER}")
    ENCODED_PASSWORD=$(url_encode "${POSTGRES_PASSWORD}")
    ENCODED_DB=$(url_encode "${POSTGRES_DB}")
    DATA_SOURCE_DNS="postgres://${ENCODED_USER}:${ENCODED_PASSWORD}@postgres:5432/${ENCODED_DB}?sslmode=disable"
fi

PROJECT_NAME=$(json_escape "${BLNK_PROJECT_NAME:-Blnk}")
DATA_SOURCE_DNS=$(json_escape "${DATA_SOURCE_DNS}")

cat > "${CONFIG_FILE}" <<EOF
{
  "project_name": "${PROJECT_NAME}",
  "data_source": {
    "dns": "${DATA_SOURCE_DNS}"
  },
  "redis": {
    "dns": "redis:6379"
  },
  "server": {
    "port": "5001"
  }
}
EOF

echo "Created ${CONFIG_FILE} from the compose environment."
echo "Edit ${CONFIG_FILE} to customize your Blnk configuration."
