# syntax=docker/dockerfile:1
FROM golang:1.16 as build-env
WORKDIR /go/src/saifu

COPY ./go.mod /go/src/saifu
COPY ./go.sum /go/src/saifu
RUN go mod download
RUN go mod verify
COPY . .
RUN CGO_ENABLED=0
RUN go install ./cmd

FROM gcr.io/distroless/base
COPY --from=build-env /go/bin/cmd /

ENTRYPOINT ["/cmd"]
CMD [ "server", "--config", "saifu.json" ]

EXPOSE 8080