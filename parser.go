package main

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

type Topic struct {
	Name              string `yaml:"name"`
	Partitions        int    `yaml:"partitions"`
	ReplicationFactor int    `yaml:"replicationFactor"`
	RetentionMs       string `yaml:"retentionMs"`
	CleanupPolicy     string `yaml:"cleanupPolicy,omitempty"`
}

type Topics struct {
	Tpcs []Topic `yaml:"topics"`
}

func getYamlData(fileName string) Topics {

	yamlFile, err := ioutil.ReadFile(fileName)
	if err != nil {
		fmt.Printf("Error reading YAML file: %s\n", err)
	}

	var topics Topics
	err = yaml.Unmarshal(yamlFile, &topics)
	if err != nil {
		fmt.Printf("Error parsing YAML file: %s\n", err)
	}

	return topics

}
