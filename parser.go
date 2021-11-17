package main

import (
	"bufio"
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"os"
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

func getMessageData(fileName string) []string {

	dataFile, err := os.Open(fileName)

	if err != nil {
		log.Fatalf("==> Failed opening data file: %s", err)
	}

	scanner := bufio.NewScanner(dataFile)
	scanner.Split(bufio.ScanLines)
	var messages []string

	for scanner.Scan() {
		messages = append(messages, scanner.Text())
	}

	err = dataFile.Close()

	return messages

}

func getDiffTopicsList(fileTopicsList, brokerTopicsList []string) []string {
  brokerTopicsMap := make(map[string]struct{}, len(brokerTopicsList))

  for _, topic := range brokerTopicsList {
      brokerTopicsMap[topic] = struct{}{}
  }

  var diffTopicsList []string

  for _, topic := range fileTopicsList {
      if _, found := brokerTopicsMap[topic]; !found {
        diffTopicsList = append(diffTopicsList, topic)
      } else {
        fmt.Printf("==> Skipping %s... %s\n", topic, "Exists")
      }
  }

  return diffTopicsList
}

