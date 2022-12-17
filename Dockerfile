# syntax=docker/dockerfile:1
FROM golang:1.17 as build-env
WORKDIR /go/src/saifu

COPY . .
RUN go build -o saifu ./cmd/*.go

FROM gcr.io/distroless/base
COPY --from=build-env /go/src/saifu/saifu .

EXPOSE 8080
CMD ["./saifu", "start"]