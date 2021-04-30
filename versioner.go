package main

import (
	"strings"
)

func versionize(topic string, enabled bool, version string) string {

	if enabled {
		if strings.HasSuffix(topic, "Commands") {
			topic = topic + "_v" + version
		}
	}

	return topic
}
