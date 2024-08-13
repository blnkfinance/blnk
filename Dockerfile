FROM golang:1.21-alpine as build-env
WORKDIR /go/src/blnk

COPY . .

RUN go build -o /blnk ./cmd/*.go

FROM debian:bullseye-slim

# Install pg_dump version 16
RUN apt-get update && apt-get install -y wget gnupg2 lsb-release && \
    echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list && \
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
    apt-get update && \
    apt-get install -y postgresql-client-16 && \
    rm -rf /var/lib/apt/lists/*

COPY --from=build-env /blnk /usr/local/bin/blnk

RUN chmod +x /usr/local/bin/blnk

CMD ["blnk", "start"]

EXPOSE 8080
