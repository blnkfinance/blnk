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
# Creates blnk.json with docker-compose defaults when missing. The generated
# file enables server.secure and a random master key. Edit blnk.json directly
# for custom configuration.

set -eu

# Keep URL parsing deterministic and encode credentials byte-by-byte.
export LC_ALL=C

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG_FILE="${ROOT_DIR}/blnk.json"

warn_if_auth_disabled() {
    file=$1
    if grep -Eq '"secure"[[:space:]]*:[[:space:]]*true' "${file}"; then
        if grep -Eq '"secret_key"[[:space:]]*:[[:space:]]*"[^"][^"]*"' "${file}"; then
            return 0
        fi
        echo "WARNING: ${file} sets server.secure but has no server.secret_key. The server will refuse to start." >&2
        return 0
    fi
    echo "WARNING: ${file} does not set server.secure to true. The server will refuse to start unless BLNK_ALLOW_INSECURE=true." >&2
}

generate_secret() {
    if command -v openssl >/dev/null 2>&1; then
        openssl rand -hex 32
        return
    fi
    od -An -N32 -tx1 /dev/urandom | tr -d ' \n'
}

if [ -f "${CONFIG_FILE}" ]; then
    warn_if_auth_disabled "${CONFIG_FILE}"
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
            *)
                # Mask to a byte. macOS sh sign-extends values above 0x7f.
                code=$(printf '%d' "'${character}")
                encoded="${encoded}$(printf '%%%02X' $((code & 255)))"
                ;;
        esac
        value=${value#?}
    done

    printf '%s' "${encoded}"
}

json_escape() {
    value=$1

    while [ -n "${value}" ]; do
        character=${value%"${value#?}"}
        case "${character}" in
            '"') printf '\\"' ;;
            '\') printf '\\\\' ;;
            [[:cntrl:]]) printf '\\u%04x' "'${character}" ;;
            *) printf '%s' "${character}" ;;
        esac
        value=${value#?}
    done
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
SECRET_KEY=$(generate_secret)
SECRET_KEY_JSON=$(json_escape "${SECRET_KEY}")

# The generated file contains the database password and the master key.
umask 077
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
    "port": "5001",
    "secure": true,
    "secret_key": "${SECRET_KEY_JSON}"
  }
}
EOF

echo "Created ${CONFIG_FILE} from the compose environment with authentication enabled."
echo "Send this master key in the X-Blnk-Key header. It is stored as server.secret_key in ${CONFIG_FILE}:"
echo "${SECRET_KEY}"
