package main

import (
	"strings"
)


func versionize(topics []string, version string) []string {

	var versionedTopics []string

	for _, topic := range topics {
		if strings.HasSuffix(topic, "Commands") {
			topic = topic + "_v" + version
		}
		versionedTopics = append(versionedTopics, topic)
	}

	return versionedTopics
}
