VERSION := 1.4
IMAGE:= quay.io/iaroki/kafka-topics:$(VERSION)

default: all

image:
	nerdctl build -t $(IMAGE) .

image-slim:
	nerdctl build -t $(IMAGE)-slim -f Dockerfile.slim .

image-all: image image-slim

push:
	nerdctl push $(IMAGE)

push-slim:
	nerdctl push $(IMAGE)-slim

push-all: push push-slim

all: image-all push-all

