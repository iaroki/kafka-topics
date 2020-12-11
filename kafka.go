package main

import (
	"context"
	"fmt"
	"log"
	"sort"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

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
		fmt.Println(topic)
	}
}

func createTopic(adminClient *kafka.AdminClient, topic Topic) {
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
