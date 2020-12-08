package main

import (
	"strings"
)

func versionize(topic string, version string) string {

	if strings.HasSuffix(topic, "Commands") {
		topic = topic + "_v" + version
	}

	return topic
}
