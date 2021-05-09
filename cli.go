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
				Usage:       "YAML formatted file with topics to load: `topics.yaml`",
				Destination: &topicFile,
				Required:    false,
			},
			&cli.StringFlag{
				Name:        "action",
				Aliases:     []string{"a"},
				Usage:       "Action to take: `add | del [force] | list | search [arg] | clean [force] | consume`",
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

				if appConfig.KafkaTopicSubscribe {
					consumerSubscribed := getConsumerSubscribed(consumerClient, []string{topic})
					consumeMessages(consumerSubscribed, messages)
				} else {
					consumerAssigned := getConsumerAssigned(consumerClient, topic)
					consumeMessages(consumerAssigned, messages)
				}

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
