package main

import (
	"fmt"
	"github.com/urfave/cli/v2"
	"log"
	"os"
	"strconv"
	"strings"
)

func initApp() {

	var action, topicFile, configFile, topicName, topicVersion string
	var confirmation, destruction bool
	var consumeMessagesCounter int

	app := &cli.App{
		Name:  "kafka-topics",
		Usage: "CLI tool for Kafka topics management",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "file",
				Aliases:     []string{"f"},
				Usage:       "File to load: `topics.yaml` or `data.log`",
				Destination: &topicFile,
				Required:    false,
			},
			&cli.StringFlag{
				Name:        "action",
				Aliases:     []string{"a"},
				Usage:       "Action to take: add/del/list/search/clean/consume/produce",
				Destination: &action,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "config",
				Aliases:     []string{"c"},
				Usage:       "YAML formatted configuration file: `config.yaml`",
				Destination: &configFile,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "topic",
				Aliases:     []string{"t"},
				Usage:       "Topic name to consume: `userEvents`",
				Destination: &topicName,
				Required:    false,
			},
			&cli.IntFlag{
				Name:        "messages",
				Aliases:     []string{"m"},
				Usage:       "Number of messages to consume: `10`",
				Destination: &consumeMessagesCounter,
				Required:    false,
			},
			&cli.StringFlag{
				Name:        "version",
				Aliases:     []string{"v"},
				Usage:       "Version of Commands topics: 16",
				Destination: &topicVersion,
				Required:    false,
			},
			&cli.BoolFlag{
				Name:        "yes",
				Aliases:     []string{"y"},
				Usage:       "Confirmation for destructive actions",
				Destination: &confirmation,
				Required:    false,
			},
			&cli.BoolFlag{
				Name:        "destroy",
				Aliases:     []string{"d"},
				Usage:       "Destroy filtered topics",
				Destination: &destruction,
				Required:    false,
			},
		},
		Action: func(c *cli.Context) error {

			appConfig := getConfig(configFile)

			switch action {

			case "add":
				if topicFile == "" {
					fmt.Println("==> Select YAML formatted topics file: `-f topics.yaml`")
					os.Exit(3)
				}

				fmt.Println("==> Adding topics to broker:", appConfig.BootstrapServers)
				topics := getYamlData(topicFile)
				adminClient := getAdminClient(appConfig)

				var version string

				if topicVersion != "" {
					version = topicVersion
				} else {
					version = strconv.Itoa(appConfig.TopicVersion)
				}

				for _, topic := range topics.Topics {
					topic.Name = versionize(topic.Name, appConfig.TopicVersioningEnabled, version) // COMMAND VERSIONIZER
					createTopic(adminClient, topic)
				}

			case "del":
				if topicFile == "" {
					fmt.Println("==> Select YAML formatted topics file: `-f topics.yaml`")
					os.Exit(3)
				}

				if confirmation {
					fmt.Println("==> Deleting topics from broker:", appConfig.BootstrapServers)
					topics := getYamlData(topicFile)
					adminClient := getAdminClient(appConfig)

					var version string

					if topicVersion != "" {
						version = topicVersion
					} else {
						version = strconv.Itoa(appConfig.TopicVersion)
					}

					for _, topic := range topics.Topics {
						topic.Name = versionize(topic.Name, appConfig.TopicVersioningEnabled, version) // COMMAND VERSIONIZER
						deleteTopic(adminClient, topic.Name)
					}
				} else {
					fmt.Println("==> Confirm destructive actions")
				}

			case "list":
				fmt.Println("==> Listing topics for broker:", appConfig.BootstrapServers)
				adminClient := getAdminClient(appConfig)
				topics := getTopicsFromBroker(adminClient)
				listTopics(topics)

			case "search":
				if c.NArg() > 0 {
					var filter string

					filter = c.Args().Get(0)
					fmt.Printf("==> Searching *%s* topics for broker: %s \n", filter, appConfig.BootstrapServers)
					adminClient := getAdminClient(appConfig)
					topics := getTopicsFromBroker(adminClient)

					var filteredTopics []string

					for _, topic := range topics {
						if strings.Contains(topic, filter) {
							filteredTopics = append(filteredTopics, topic)
						}
					}

					listTopics(filteredTopics)

					if destruction {
						fmt.Printf("\n==> Topics selected for destruction\n")
						if confirmation {
							for _, topic := range filteredTopics {
								deleteTopic(adminClient, topic)
							}
						} else {
							fmt.Println("==> Confirm destructive actions")
						}
					}
				} else {
					fmt.Println("==> Add some arguments for search")
				}

			case "clean":
				if confirmation {
					fmt.Println("==> Cleaning topics for broker:", appConfig.BootstrapServers)
					adminClient := getAdminClient(appConfig)
					topics := getTopicsFromBroker(adminClient)

					for _, topic := range topics {
						deleteTopic(adminClient, topic)
					}
				} else {
					fmt.Println("==> Confirm destructive actions")
				}

			case "consume":
				consumerClient := getConsumerClient(appConfig)
				var topic string
				var messages int

				if topicName != "" {
					topic = topicName
				} else {
					topic = appConfig.KafkaTopic
				}

				if consumeMessagesCounter != 0 {
					messages = consumeMessagesCounter
				} else {
					messages = appConfig.ConsumeMessagesCounter
				}

				fmt.Printf("==> Consuming messages from %s at %s\n", topic, appConfig.BootstrapServers)

				if appConfig.KafkaTopicSubscribe {
					consumerSubscribed := getConsumerSubscribed(consumerClient, []string{topic})
					consumeMessages(consumerSubscribed, messages)
				} else {
					consumerAssigned := getConsumerAssigned(consumerClient, topic)
					consumeMessages(consumerAssigned, messages)
				}

			case "produce":
				if topicFile == "" {
					fmt.Println("==> Select data file: `-f data.log`")
					os.Exit(3)
				}

				var topic string

				if topicName != "" {
					topic = topicName
				} else {
					topic = appConfig.KafkaTopic
				}

				fmt.Printf("==> Producing messages to %s at %s\n", topic, appConfig.BootstrapServers)

				messageData := getMessageData(topicFile)
				producerClient := getProducerClient(appConfig)
				produceMessages(producerClient, topic, messageData)

			default:
				fmt.Println("==> Wrong arguments... Try help")
			}

			return nil
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
