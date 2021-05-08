package main

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

type yamlTopic struct {
	Name              string `yaml:"name"`
	Partitions        int    `yaml:"partitions"`
	ReplicationFactor int    `yaml:"replicationFactor"`
	RetentionMs       string `yaml:"retentionMs"`
	CleanupPolicy     string `yaml:"cleanupPolicy,omitempty"`
}

type yamlTopics struct {
	Topics []yamlTopic `yaml:"topics"`
}

func getYamlData(fileName string) yamlTopics {

	yamlFile, err := ioutil.ReadFile(fileName)
	if err != nil {
		fmt.Printf("Error reading YAML file: %s\n", err)
	}

	var topics yamlTopics
	err = yaml.Unmarshal(yamlFile, &topics)
	if err != nil {
		fmt.Printf("Error parsing YAML file: %s\n", err)
	}

	return topics

}
