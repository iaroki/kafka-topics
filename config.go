package main

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

type Config struct {
	BootstrapServers       string `yaml:"bootstrap.servers"`
	SaslMechanism          string `yaml:"sasl.mechanism"`
	SecurityProtocol       string `yaml:"security.protocol"`
	SslTruststoreLocation  string `yaml:"ssl.truststore.location"`
	KafkaUsername          string `yaml:"kafka.username"`
	KafkaPassword          string `yaml:"kafka.password"`
	KafkaTopic             string `yaml:"kafka.topic"`
	KafkaTopicSubscribe    bool   `yaml:"kafka.topic.subscribe"`
	KafkaConsumerGroup     string `yaml:"kafka.consumer.group"`
	AutoOffsetReset        string `yaml:"auto.offset.reset"`
	TopicVersioningEnabled bool   `yaml:"topic.versioning.enabled"`
	TopicVersion           int    `yaml:"topic.version"`
	ConsumeMessageNumber   int    `yaml:"consume.message.number"`
}

func getConfig(configFilePath string) Config {

	yamlFile, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		fmt.Printf("Error reading YAML file: %s\n", err)
	}

	var appConfig Config

	err = yaml.Unmarshal(yamlFile, &appConfig)
	if err != nil {
		fmt.Printf("Error parsing YAML file: %s\n", err)
	}

	return appConfig

}
