VERSION := 1.4
IMAGE:= iaroki/kafka-topics:$(VERSION)

default: all

image:
	docker build -t $(IMAGE) .

image-slim:
	docker build -t $(IMAGE)-slim -f Dockerfile.slim .

image-all: image image-slim

push:
	docker push $(IMAGE)

push-slim:
	docker push $(IMAGE)-slim

push-all: push push-slim

all: image-all push-all

