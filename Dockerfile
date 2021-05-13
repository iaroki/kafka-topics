FROM golang:1.16 as builder
ARG APP="kafka-topics"
WORKDIR $GOPATH/src/$APP
COPY . .
RUN go mod tidy && \
    go build -o $APP . && \
    mv $APP /bin/

FROM gcr.io/distroless/base
COPY --from=builder /etc/ssl/certs /etc/ssl/certs
COPY --from=builder /bin/$APP /bin
ENTRYPOINT ["kafka-topics"]
