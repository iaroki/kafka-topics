FROM golang:1.15
ARG APP="kafka-topics"
WORKDIR $GOPATH/src/$APP
COPY . .
RUN go mod download && \
    go build -o $APP . && \
    mv $APP /bin/
ENTRYPOINT ["kafka-topics"]
