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

	var action, topicFile string
	var confirmation bool

	app := &cli.App{
		Name:  "kafka-topics",
		Usage: "manage Kafka topics",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "file",
				Aliases:     []string{"f"},
				Usage:       "file to load [ topics.yaml ]",
				Destination: &topicFile,
				Required:    false,
			},
			&cli.StringFlag{
				Name:        "action",
				Aliases:     []string{"a"},
				Usage:       "action to take [ add | del [force] | list | search [arg] | clean [force] ]",
				Destination: &action,
				Required:    true,
			},
			&cli.BoolFlag{
				Name:        "yes",
				Aliases:     []string{"y"},
				Usage:       "confirmation [ yes ]",
				Destination: &confirmation,
				Required:    false,
			},
		},
		Action: func(c *cli.Context) error {
			adminClient := getAdminClient()
			switch action {
			case "add":
				fmt.Println("Adding topics to broker:", kafkaBroker)
				topics := getYamlData(topicFile)
				for _, topic := range topics.Tpcs {
					topic.Name = versionize(topic.Name, topicVersion) // COMMAND VERSIONIZER
					//createTopic(adminClient, topic.Name, topic.Partitions, topic.ReplicationFactor, topic.RetentionMs, topic.CleanupPolicy)
					createTopic(adminClient, topic)
				}
			case "del":
				if confirmation {
					fmt.Println("Deleting topics from broker:", kafkaBroker)
					topics := getYamlData(topicFile)
					for _, topic := range topics.Tpcs {
						topic.Name = versionize(topic.Name, topicVersion) // COMMAND VERSIONIZER
						deleteTopic(adminClient, topic.Name)
					}
				}
			case "list":
				fmt.Println("Listing topics for broker:", kafkaBroker)
				topics := getTopicsFromBroker(adminClient)
				listTopics(topics)
			case "search":
				if c.NArg() > 0 {
					var filter string
					filter = c.Args().Get(0)
					fmt.Printf("Searching *%s* topics for broker: %s \n", filter, kafkaBroker)
					topics := getTopicsFromBroker(adminClient)
					var filteredTopics []string
					for _, topic := range topics {
						if strings.Contains(topic, filter) {
							filteredTopics = append(filteredTopics, topic)
						}
					}
					listTopics(filteredTopics)
				}
			case "clean":
				if confirmation {
					fmt.Println("Cleaning topics for broker:", kafkaBroker)
					topics := getTopicsFromBroker(adminClient)
					for _, topic := range topics {
						deleteTopic(adminClient, topic)
					}
				}
			default:
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
