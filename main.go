package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/urfave/cli/v2"
)

var topicVersion = os.Getenv("TOPIC_VERSION")
var kafkaBroker = os.Getenv("KAFKA_BROKER")
var kafkaUser = os.Getenv("KAFKA_USER")
var kafkaPass = os.Getenv("KAFKA_PASS")

func initApp() {

	var action string
	var topicFile string

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
			&cli.StringFlag{
				Name:        "file",
				Aliases:     []string{"f"},
				Usage:       "file to load [ topics.yaml ]",
				Destination: &topicFile,
				Required:    false,
			},
		},
		Action: func(c *cli.Context) error {

			if action == "add" {
				fmt.Println("Adding topics to broker:", kafkaBroker)
				adminClient := getAdminClient()
				topics := getYamlData(topicFile)
				for _, topic := range topics.Tpcs {
					topicName := versionize(topic.Name, topicVersion) // COMMAND VERSIONIZER
					createTopic(adminClient, topicName, topic.Partitions, topic.ReplicationFactor, topic.RetentionMs, topic.CleanupPolicy)
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
