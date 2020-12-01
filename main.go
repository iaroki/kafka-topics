package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/urfave/cli/v2"
)

var Version string = "12"
var topicFile string = "topics.txt"
var kafkaBroker string = os.Getenv("KAFKA_BROKER")
var kafkaUser string = os.Getenv("KAFKA_USER")
var kafkaPass string = os.Getenv("KAFKA_PASS")

func getAdminClient() *kafka.AdminClient {
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBroker,
		"sasl.mechanisms":   "PLAIN",
		"security.protocol": "SASL_PLAINTEXT",
		"sasl.username":     kafkaUser,
		"sasl.password":     kafkaPass})

	if err != nil {
		log.Fatalf("Failed to create AdminClient: %s", err)
	}

	return adminClient
}

func listTopics(adminClient *kafka.AdminClient) {
	metadata, err := adminClient.GetMetadata(nil, true, 3000)

	if err != nil {
		log.Fatalf("Metadata error: %s", err)
	}

	var mydata []string

	for topic := range metadata.Topics {
		mydata = append(mydata, topic)

	}
	sort.Strings(mydata)

	for _, topic := range mydata {
		//log.Printf("%s %d\n", topic, len(metadata.Topics[topic].Partitions))
		fmt.Println(topic)
	}
}

func createTopic(adminClient *kafka.AdminClient, name string, retention string) {
	topicConfig := map[string]string{"retention.ms": retention}
	topicSpec := kafka.TopicSpecification{Topic: name, NumPartitions: 60, ReplicationFactor: 3, Config: topicConfig}
	topicSpecs := []kafka.TopicSpecification{topicSpec}
	ctx := context.Background()
	result, err := adminClient.CreateTopics(ctx, topicSpecs)

	if err != nil {
		log.Fatalf("Topic create error: %s", err)
	}

	fmt.Printf("Creating %s... %s\n", result[0].Topic, result[0].Error)
}

func deleteTopic(adminClient *kafka.AdminClient, name string) {
	topics := []string{name}
	ctx := context.Background()
	result, err := adminClient.DeleteTopics(ctx, topics)

	if err != nil {
		log.Fatalf("Topic delete error: %s", err)
	}

	fmt.Printf("Deleting %s... %s\n", result[0].Topic, result[0].Error)
}

func getTopicsFromFile(filePath string, version string) []string {
	file, err := os.Open(filePath)

	if err != nil {
		log.Fatalf("Failed opening file: %s", err)
	}

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	var fileTopics []string

	for scanner.Scan() {
		topic := scanner.Text()
		if strings.HasSuffix(topic, "Commands") {
			topic = topic + "_v" + version
		}
		fileTopics = append(fileTopics, topic)
	}

	file.Close()

	return fileTopics
}

func initApp() {

	var action string
	var environment string

	app := &cli.App{
		Name:  "kafka-topics",
		Usage: "manage Kafka topics",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "action",
				Aliases:     []string{"a"},
				Usage:       "action to take [ add | del | list ]",
				Destination: &action,
				Required:    true,
			},
		},
		Action: func(c *cli.Context) error {

			if action == "add" {
				fmt.Println("Adding to environment:", environment)
				topics := getTopicsFromFile(topicFile, Version)
				for _, topic := range topics {
					adminClient := getAdminClient()
					createTopic(adminClient, topic, "-1")

				}
			} else if action == "del" {
				fmt.Println("Deleting from environment:", environment)
				topics := getTopicsFromFile(topicFile, Version)
				for _, topic := range topics {
					adminClient := getAdminClient()
					deleteTopic(adminClient, topic)

				}
			} else if action == "list" {
				fmt.Println("Listing for environment:", environment)
				adminClient := getAdminClient()
				listTopics(adminClient)
			} else {
				fmt.Println("Wrong arguments... Try help")
			}
			return nil
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func parser() {
	//os.A
}

func main() {

	adminClient := getAdminClient()
	listTopics(adminClient)
	//fmt.Println("#############################################")
	//topics := getTopicsFromFile(topicFile, Version)
	//for _, topic := range topics {
	//	fmt.Println(topic)
	//if strings.HasSuffix(topic, "_v14") {
	//	deleteTopic(adminClient, topic)
	//}
		//createTopic(adminClient, topic, "-1")
	//deleteTopic(adminClient, topic)
	//}
	//createTopic(adminClient, "test", "-1")
	//deleteTopic(adminClient, "test")

	//initApp()
}
