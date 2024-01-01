#!/bin/bash

# Step 1: Check if convoy is already running
if ! docker ps | grep -q convoy; then
    # Check if convoy directory exists
    if [ ! -d "convoy" ]; then
        # Clone the convoy repository
        git clone https://github.com/frain-dev/convoy.git
    fi

    # Step 3: Go to the Convoy folder
    # shellcheck disable=SC2164
    cd convoy

    # Step 4: Start Services using Docker Compose
    docker compose -f configs/local/docker-compose.yml up
else
    echo "Convoy is already running. No action needed."
fi


# Step 1: Run bknk binary
./blnk

