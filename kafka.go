package main

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	"log"
	"os"
	"sort"
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
		log.Fatalf("==> Failed to create AdminClient: %s", err)
	}

	return adminClient
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

func getProducerClient(consumerConfig Config) *kafka.Producer {

	hostname, err := os.Hostname()
	clientId := "kafka-topics-" + hostname
	producerClient, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": consumerConfig.BootstrapServers,
		"sasl.mechanism":    consumerConfig.SaslMechanism,
		"security.protocol": consumerConfig.SecurityProtocol,
		"ssl.ca.location":   consumerConfig.SslTruststoreLocation,
		"sasl.username":     consumerConfig.KafkaUsername,
		"sasl.password":     consumerConfig.KafkaPassword,
		"partitioner":       "murmur2_random",
		"client.id":         clientId,
		"acks":              "all"})

	if err != nil {
		log.Fatal(err)
	}

	return producerClient
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
		log.Fatalf("==> Topic create error: %s", err)
	}

	fmt.Printf("==> Creating %s... %s\n", result[0].Topic, result[0].Error)
}

func deleteTopic(adminClient *kafka.AdminClient, name string) {

	topics := []string{name}
	result, err := adminClient.DeleteTopics(context.Background(), topics)

	if err != nil {
		log.Fatalf("==> Topic delete error: %s", err)
	}

	fmt.Printf("==> Deleting %s... %s\n", result[0].Topic, result[0].Error)
}

func getTopicsFromBroker(adminClient *kafka.AdminClient) []string {

	metadata, err := adminClient.GetMetadata(nil, true, 5000)

	if err != nil {
		log.Fatalf("==> Metadata error: %s", err)
	}

	var topicsMetadata []string

	for topic := range metadata.Topics {
		topicsMetadata = append(topicsMetadata, topic)

	}

	return topicsMetadata
}

func getTopicPartitions(adminClient *kafka.AdminClient, topic string) []kafka.PartitionMetadata {

	metadata, err := adminClient.GetMetadata(&topic, false, 5000)

	if err != nil {
		log.Fatalf("==> Metadata error: %s", err)
	}

	topicError := metadata.Topics[topic].Error.String()

	if topicError != "Success" {
		os.Exit(3)
	}

	partsMetadata := metadata.Topics[topic].Partitions

	return partsMetadata

}

func getConsumerSubscribed(consumerClient *kafka.Consumer, topics []string) *kafka.Consumer {

	err := consumerClient.SubscribeTopics(topics, nil)

	if err != nil {
		log.Fatal(err)
	}

	return consumerClient
}

func getConsumerAssigned(consumerClient *kafka.Consumer, topic string) *kafka.Consumer {

	adminClient, err := kafka.NewAdminClientFromConsumer(consumerClient)

	if err != nil {
		log.Fatal(err)
	}

	partitionsMetadata := getTopicPartitions(adminClient, topic)
	var topicPartitions []kafka.TopicPartition

	for part := range partitionsMetadata {
		tp := kafka.TopicPartition{Topic: &topic, Partition: int32(part)}
		topicPartitions = append(topicPartitions, tp)
	}

	err = consumerClient.Assign(topicPartitions)

	if err != nil {
		log.Fatal(err)
	}

	return consumerClient
}

func consumeMessages(consumerClient *kafka.Consumer, consumeMessagesCounter int) {

	run := true
	counter := 0

	for run == true {
		ev := consumerClient.Poll(0)
		switch e := ev.(type) {
		case *kafka.Message:
			counter++
			fmt.Println(string(e.Value), counter)
			if counter == consumeMessagesCounter {
				run = false
			}
		case kafka.Error:
			fmt.Println("ERROR: ", e)
			run = false
		}
	}

	err := consumerClient.Close()

	if err != nil {
		log.Fatal(err)
	}
}

func produceMessages(producerClient *kafka.Producer, topic string, messages []string) {

	for index, message := range messages {
		err := producerClient.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Key:   []byte(uuid.New().String()),
			Value: []byte(message),
		}, nil)

		if err != nil {
			fmt.Printf("==> Cannot produce message %s", err)
		}

		fmt.Println(message, index+1)

	}

	producerClient.Flush(3000)
}
