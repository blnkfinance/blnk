# syntax=docker/dockerfile:1
FROM golang:1.20.3 as build-env
WORKDIR /go/src/blnk

COPY . .
RUN CGO_ENABLED=0 go build -o blnk ./cmd/*.go

FROM gcr.io/distroless/base
COPY --from=build-env /go/src/blnk/blnk .

EXPOSE 5001
CMD ["./blnk", "start"]