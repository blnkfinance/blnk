# syntax=docker/dockerfile:1
FROM golang:1.20.3 as build-env
WORKDIR /go/src/blnk

COPY . .

RUN go mod download
RUN go mod verify

RUN go build -o blnk ./cmd/*.go

FROM gcr.io/distroless/base
COPY --from=build-env /go/src/blnk/blnk .
COPY ui /ui

CMD ["./blnk", "start"]

EXPOSE 8080