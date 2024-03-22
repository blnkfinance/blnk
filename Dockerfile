RUN go build -o blnk ./cmd/*.go

FROM gcr.io/distroless/base
COPY --from=build-env /go/src/blnk/blnk .


CMD ["./blnk", "start"]

EXPOSE 8080