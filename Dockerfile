FROM golang:1.16 as builder
ARG APP="kafka-topics"
WORKDIR $GOPATH/src/$APP
COPY . .
RUN go mod tidy && \
    go build -o $APP . && \
    mv $APP /bin/

FROM debian:bullseye-slim
COPY --from=builder /bin/$APP /bin
ENTRYPOINT ["kafka-topics"]
