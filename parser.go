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
	CleanupPolicy     string `yaml:"cleanupPolicy"`
}

type Topics struct {
	Tpcs []Topic `topics`
}


func getYamlData(fileName string) Topics {
	//fmt.Println("Parsing YAML file")
	//
	//var fileName string
	//flag.StringVar(&fileName, "f", "", "YAML file to parse.")
	//flag.Parse()
	//
	//if fileName == "" {
	//	fmt.Println("Please provide yaml file by using -f option")
	//	return
	//}

	//fileName := "topics.yaml"
	yamlFile, err := ioutil.ReadFile(fileName)
	if err != nil {
		fmt.Printf("Error reading YAML file: %s\n", err)
	}

	var topics Topics
	err = yaml.Unmarshal(yamlFile, &topics)
	if err != nil {
		fmt.Printf("Error parsing YAML file: %s\n", err)
	}

	//for i, topic := range topics.Tpcs {
	//	fmt.Println(i, topic.Name, topic.Partitions, topic.ReplicationFactor, topic.RetentionMs, topic.CleanupPolicy)
	//}

	return topics

}
