FROM golang:1.15 as builder
ARG APP="kafka-topics"
WORKDIR $GOPATH/src/$APP
COPY . .
RUN go mod download && \
    go build -o $APP . && \
    mv $APP /bin/

FROM debian:buster-slim
COPY --from=builder /bin/$APP /bin
ENTRYPOINT ["kafka-topics"]
