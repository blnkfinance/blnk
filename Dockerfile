# syntax=docker/dockerfile:1
FROM golang:1.20.3 as build-env
WORKDIR /go/src/blnk

COPY . .

RUN go mod download
RUN go mod verify

RUN go build -o blnk ./cmd/*.go

FROM alpine:3.15
# Install pg_dump
RUN apk add --no-cache postgresql-client

COPY --from=build-env /go/src/blnk/blnk .

CMD ["./blnk", "start"]
EXPOSE 8080