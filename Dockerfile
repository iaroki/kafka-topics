FROM golang:1.16 as builder
ARG APP="kafka-topics"
WORKDIR $GOPATH/src/$APP
COPY . .
RUN go mod tidy && \
    go build -o $APP . && \
    mv $APP /bin/

FROM gcr.io/distroless/base@sha256:0d7168393f3b5182e0b4573b181498ba592ab18d14f35e299c2375ead522254a
COPY --from=builder /etc/ssl/certs /etc/ssl/certs
COPY --from=builder /bin/$APP /bin
ENTRYPOINT ["kafka-topics"]
