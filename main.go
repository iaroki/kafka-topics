package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/urfave/cli/v2"
)

var topicVersion = os.Getenv("TOPIC_VERSION")
var topicFile = os.Getenv("TOPIC_FILE")
var kafkaBroker = os.Getenv("KAFKA_BROKER")
var kafkaUser = os.Getenv("KAFKA_USER")
var kafkaPass = os.Getenv("KAFKA_PASS")

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

func listTopics(topics []string) {

	sort.Strings(topics)

	for _, topic := range topics {
		//log.Printf("%s %d\n", topic, len(metadata.Topics[topic].Partitions))
		fmt.Println(topic)
	}
}

func createTopic(adminClient *kafka.AdminClient, name string, partitions int, replicationFactor int, retention string, cleanupPolicy string) {
	topicConfig := map[string]string{"retention.ms": retention, "cleanup.policy": cleanupPolicy}
	topicSpec := kafka.TopicSpecification{Topic: name, NumPartitions: partitions, ReplicationFactor: replicationFactor, Config: topicConfig}
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

func getTopicsFromBroker(adminClient *kafka.AdminClient) []string {
	metadata, err := adminClient.GetMetadata(nil, true, 3000)

	if err != nil {
		log.Fatalf("Metadata error: %s", err)
	}

	var topicsMetadata []string

	for topic := range metadata.Topics {
		topicsMetadata = append(topicsMetadata, topic)

	}

	return topicsMetadata
}

//func getTopicsFromFile(filePath string, version string) []string {
//
//	absolutePath, err := filepath.Abs(filePath)
//	if err != nil {
//		log.Fatalf("File did not detected: %s", err)
//	}
//	fmt.Println("Opening topic file:", absolutePath)
//	file, err := os.Open(absolutePath)
//
//	if err != nil {
//		log.Fatalf("Failed opening file: %s", err)
//	}
//
//	scanner := bufio.NewScanner(file)
//	scanner.Split(bufio.ScanLines)
//	var fileTopics []string
//
//	for scanner.Scan() {
//		topic := scanner.Text()
//		if strings.HasSuffix(topic, "Commands") {
//			topic = topic + "_v" + version
//		}
//		fileTopics = append(fileTopics, topic)
//	}
//
//	_ = file.Close()
//
//	return fileTopics
//}

func initApp() {

	var action string

	app := &cli.App{
		Name:  "kafka-topics",
		Usage: "manage Kafka topics",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "action",
				Aliases:     []string{"a"},
				Usage:       "action to take [ add | del | list | search [arg] | clean [force] ]",
				Destination: &action,
				Required:    true,
			},
		},
		Action: func(c *cli.Context) error {

			if action == "add" {
				fmt.Println("Adding topics to broker:", kafkaBroker)
				adminClient := getAdminClient()
				//topics := getTopicsFromFile(topicFile, topicVersion)
				topics := getYamlData(topicFile)
				for _, topic := range topics.Tpcs {
					topicName := versionize(topic.Name, topicVersion) // COMMAND VERSIONIZER
					createTopic(adminClient, topicName, topic.Partitions, topic.ReplicationFactor, topic.RetentionMs, topic.CleanupPolicy)
					//fmt.Println(topic.Name, topic.Partitions, topic.ReplicationFactor, topic.RetentionMs, topic.CleanupPolicy)
				}

			} else if action == "del" {
				fmt.Println("Deleting topics from broker:", kafkaBroker)
				adminClient := getAdminClient()
				topics := getYamlData(topicFile)
				for _, topic := range topics.Tpcs {
					topicName := versionize(topic.Name, topicVersion) // COMMAND VERSIONIZER
					deleteTopic(adminClient, topicName)

				}
			} else if action == "list" {
				fmt.Println("Listing topics for broker:", kafkaBroker)
				adminClient := getAdminClient()
				topics := getTopicsFromBroker(adminClient)
				listTopics(topics)
			} else if action == "search" {
				if c.NArg() > 0 {
					var filter string
					filter = c.Args().Get(0)
					fmt.Printf("Searching *%s* topics for broker: %s \n", filter, kafkaBroker)
					adminClient := getAdminClient()
					topics := getTopicsFromBroker(adminClient)
					var filteredTopics []string
					for _, topic := range topics {
						if strings.Contains(topic, filter) {
							filteredTopics = append(filteredTopics, topic)
						}
					}
					listTopics(filteredTopics)
				}
			} else if action == "clean" {
				if c.NArg() > 0 {
					if c.Args().Get(0) == "force" {
						fmt.Println("Cleaning topics for broker:", kafkaBroker)
						adminClient := getAdminClient()
						topics := getTopicsFromBroker(adminClient)
						for _, topic := range topics {

							deleteTopic(adminClient, topic)

						}
					}
				}

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

func main() {

	initApp()
}
