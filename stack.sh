#!/bin/bash
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
# This script simplifies work with the docker-compose stack, execute it without parameters for help.

# Error handling
set -o errexit          # Exit on most errors
set -o errtrace         # Make sure any error trap is inherited
set -o pipefail         # Use last non-zero exit code in a pipeline
#set -o nounset          # Exits if any of the variables is not set

GRN='\033[0;32m' && YEL='\033[1;33m' && RED='\033[0;31m' && BLU='\033[0;34m' && NC='\033[0m'

declare env=".env"                  # name of the standard environment file
declare example=".env.example"      # name of the sample environment file
declare COMPOSE_CL="docker-compose" # docker-compose command syntax varies. You can override it in the .env

help() {
    printf "\n \
    Usage:${BLU} ${0} ${GRN}parameters${NC}\n \
    ${GRN}--pull, -p${NC}\t\t Pull the repo from registry\n \
    ${GRN}--up,-u${NC}\t\t Spin up\n \
    ${GRN}--build,-b${NC}\t\t Build the stack\n \
    ${GRN}--down,-d${NC}\t\t Shut down\n \
    ${GRN}--restart,-r${NC}\t Cold-restart\n \
    ${GRN}--init,-i${NC}\t\t Create a .env file\n \
    \n \
    Examples:
    ${BLU} ${0} ${GRN}-u${NC}\n\
    \n\
    "
}

showenv() {
    if [ -r ${env} ]
    then
        source ${env}
    fi
    printf "\n ==== Environment ====\n"
    printf "      Stack name : ${YEL}$COMPOSE_PROJECT_NAME${NC}\n"
    printf "    Compose file : ${BLU}$COMPOSE_FILE${NC}\n"
    printf "  CL parameter 0 : ${BLU}${0}${NC}\n"
    printf "  CL parameter 1 : ${BLU}${1}${NC}\n"
    printf "  CL parameter 2 : ${BLU}${2}${NC}\n"
    printf "  CL parameter 3 : ${BLU}${3}${NC}\n"
    if [ -r ${env} ]
    then
      printf "             env : ${BLU}${env}${NC}\n"
    else
      printf "             env : ${RED}${env}${NC} not found. You may want to initialize the stack with -i parameter\n"
    fi
    printf " =====================\n"
}

main() {
    case "${1}" in
        --pull | -p )
            docker image prune -a --force --filter "until=72h"
            ${COMPOSE_CL} --env-file ${env} -f ${COMPOSE_FILE} pull "${@:2}"
            ;;
        --up | -u )
            ${COMPOSE_CL} --env-file ${env} -f ${COMPOSE_FILE} up -d "${@:2}"
            ;;
        --down | -d )
            ${COMPOSE_CL} --env-file ${env} -f ${COMPOSE_FILE} down
            ;;
        --build | -b )
            ${COMPOSE_CL} --env-file ${env} -f ${COMPOSE_FILE} up -d --build "${@:2}"
            ;;
        --restart | -r )
            ${COMPOSE_CL} --env-file ${env} -f ${COMPOSE_FILE} down
            ${COMPOSE_CL} --env-file ${env} -f ${COMPOSE_FILE} up -d "${@:2}"
            ;;
        --init | -i )
    	    #checking if .env is available:
            if [ -r ${env} ]
            then
                printf "The environment file ${RED}${env}${NC} is already available. If you want to start from scratch, delete it and restart.\n"
            else
                # copying _env into the .env if not found:
                printf "Creating file: ${YEL}${env}${NC} with secrets... Check it before you spin up the stack.\n"
                cp ${example} ${env}
                POSTGRES_PASSWORD=$(openssl rand -base64 15)
                sed -i "s|{POSTGRES_PASSWORD}|$POSTGRES_PASSWORD|g" ${env}
                #checking if .env created successfully:
                if [ ! -r ${env} ]
                then
                    printf "Error creating environment file ${RED}${env}${NC}. Please, check if an ${BLU}.env${NC} file available, resolve and restart.\n"
                    exit 1
                fi
            fi
            ;;
        * ) help
            ;;
    esac

}

showenv "$@"
time main "$@" # calls the main procedure and prints time used to execute
