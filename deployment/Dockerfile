##################################################################################
#                                                                                #
# Blnk Server image                                                             #
# Runs DB migrations then starts the HTTP API ("blnk migrate up && blnk start") #
#                                                                                #
# To override the ARG defaults: --build-arg <varname>=<value>                   #
#                                                                                #
##################################################################################

ARG DOCKER_REPO_URL=tala-tala-dockerhub.jfrog.io

##################################################################################
#                                                                                #
#            Stage 1: Build the blnk binary                                     #
#                                                                                #
##################################################################################

FROM ${DOCKER_REPO_URL}/golang:1.25-alpine AS build-env
WORKDIR /go/src/blnk

COPY . .

RUN go build -o /blnk ./cmd/*.go

##################################################################################
#                                                                                #
#            Stage 2: Runtime image                                             #
#                                                                                #
##################################################################################

ARG DOCKER_REPO_URL=tala-tala-dockerhub.jfrog.io
FROM ${DOCKER_REPO_URL}/debian:bullseye-slim

# pg_dump (matching the Postgres major version blnk targets) is required by the
# /backup and /backup-s3 admin endpoints exposed by the server process.
RUN apt-get update && apt-get install -y wget gnupg2 lsb-release && \
    echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list && \
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
    apt-get update && \
    apt-get install -y postgresql-client-16 && \
    rm -rf /var/lib/apt/lists/*

COPY --from=build-env /blnk /usr/local/bin/blnk
RUN chmod +x /usr/local/bin/blnk

# Declare application user variables
ARG CONTAINER_USER_NAME=blnk
ARG CONTAINER_USER_ID=10001

# Create application user
RUN id -u $CONTAINER_USER_NAME 2>/dev/null || useradd --system --create-home \
    --uid $CONTAINER_USER_ID --gid 0 $CONTAINER_USER_NAME

EXPOSE 5001

USER $CONTAINER_USER_NAME

# blnk.json is mounted at runtime from the blnk-config ConfigMap (see helm values)
ENTRYPOINT ["/bin/sh", "-c"]
CMD ["blnk migrate up --config /blnk.json && blnk start --config /blnk.json"]
