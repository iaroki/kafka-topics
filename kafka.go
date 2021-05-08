package main

import (
	"context"
	"fmt"
	"log"
	"sort"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func getAdminClient(brokerConfig Config) *kafka.AdminClient {
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": brokerConfig.BootstrapServers,
		"sasl.mechanism":    brokerConfig.SaslMechanism,
		"security.protocol": brokerConfig.SecurityProtocol,
		"ssl.ca.location":   brokerConfig.SslTruststoreLocation,
		"sasl.username":     brokerConfig.KafkaUsername,
		"sasl.password":     brokerConfig.KafkaPassword})

	if err != nil {
		log.Fatalf("Failed to create AdminClient: %s", err)
	}

	return adminClient
}
func listTopics(topics []string) {

	sort.Strings(topics)

	for _, topic := range topics {
		fmt.Println(topic)
	}
}

func createTopic(adminClient *kafka.AdminClient, topic yamlTopic) {
	topicConfig := map[string]string{
		"retention.ms":   topic.RetentionMs,
		"cleanup.policy": topic.CleanupPolicy}
	topicSpec := kafka.TopicSpecification{
		Topic:             topic.Name,
		NumPartitions:     topic.Partitions,
		ReplicationFactor: topic.ReplicationFactor,
		Config:            topicConfig}
	topicSpecs := []kafka.TopicSpecification{topicSpec}
	result, err := adminClient.CreateTopics(context.Background(), topicSpecs)

	if err != nil {
		log.Fatalf("Topic create error: %s", err)
	}

	fmt.Printf("Creating %s... %s\n", result[0].Topic, result[0].Error)
}

func deleteTopic(adminClient *kafka.AdminClient, name string) {
	topics := []string{name}
	result, err := adminClient.DeleteTopics(context.Background(), topics)

	if err != nil {
		log.Fatalf("Topic delete error: %s", err)
	}

	fmt.Printf("Deleting %s... %s\n", result[0].Topic, result[0].Error)
}

func getTopicsFromBroker(adminClient *kafka.AdminClient) []string {
	metadata, err := adminClient.GetMetadata(nil, true, 5000)

	if err != nil {
		log.Fatalf("Metadata error: %s", err)
	}

	var topicsMetadata []string

	for topic := range metadata.Topics {
		topicsMetadata = append(topicsMetadata, topic)

	}

	return topicsMetadata
}

func getConsumerClient(consumerConfig Config) *kafka.Consumer {

	consumerClient, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": consumerConfig.BootstrapServers,
		"sasl.mechanism":    consumerConfig.SaslMechanism,
		"security.protocol": consumerConfig.SecurityProtocol,
		"ssl.ca.location":   consumerConfig.SslTruststoreLocation,
		"sasl.username":     consumerConfig.KafkaUsername,
		"sasl.password":     consumerConfig.KafkaPassword,
		"group.id":          consumerConfig.KafkaConsumerGroup,
		"auto.offset.reset": consumerConfig.AutoOffsetReset})
	if err != nil {
		log.Fatal(err)
	}

	return consumerClient

}

func getConsumerSubscribed(consumerClient *kafka.Consumer, topics []string) *kafka.Consumer {
	err := consumerClient.SubscribeTopics(topics, nil)
	if err != nil {
		log.Fatal(err)
	}

	return consumerClient
}

func consumeMessages(consumerClient *kafka.Consumer) {

	run := true

	for run == true {
		ev := consumerClient.Poll(0)
		switch e := ev.(type) {
		case *kafka.Message:
			fmt.Println(string(e.Value))
		case kafka.Error:
			fmt.Println("ERROR: ", e)
			run = false
		}
	}

}
