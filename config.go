package main

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

type Config struct {
	BootstrapServers       string `yaml:"bootstrap_servers"`
	SaslMechanism          string `yaml:"sasl_mechanism"`
	SecurityProtocol       string `yaml:"security_protocol"`
	SSLCALocation          string `yaml:"ssl_ca_location"`
	SSLKeyLocation         string `yaml:"ssl_key_location"`
	SSLKeyPassword         string `yaml:"ssl_key_password"`
	SSLCertificateLocation string `yaml:"ssl_certificate_location"`
	KafkaUsername          string `yaml:"kafka_username"`
	KafkaPassword          string `yaml:"kafka_password"`
	KafkaTopic             string `yaml:"kafka_topic"`
	KafkaTopicSubscribe    bool   `yaml:"kafka_topic_subscribe"`
	KafkaConsumerGroup     string `yaml:"kafka_consumer_group"`
	AutoOffsetReset        string `yaml:"auto_offset_reset"`
	TopicVersioningEnabled bool   `yaml:"topic_versioning_enabled"`
	TopicVersion           int    `yaml:"topic_version"`
	ConsumeMessagesCounter int    `yaml:"consume_messages_counter"`
}

func getConfig(configFilePath string) Config {

	yamlFile, err := ioutil.ReadFile(configFilePath)

	if err != nil {
		fmt.Printf("==> Error reading YAML file: %s\n", err)
	}

	var appConfig Config

	err = yaml.Unmarshal(yamlFile, &appConfig)

	if err != nil {
		fmt.Printf("==> Error parsing YAML file: %s\n", err)
	}

	return appConfig

}
