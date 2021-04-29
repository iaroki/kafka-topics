package main

import (
	"os"
)

var topicVersion = os.Getenv("TOPIC_VERSION")
var kafkaBroker = os.Getenv("KAFKA_BROKER")
var kafkaUser = os.Getenv("KAFKA_USER")
var kafkaPass = os.Getenv("KAFKA_PASS")

func main() {

	initApp()
}
