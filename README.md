# kafka-topics

CLI tool for Apache Kafka topic management

```
NAME:
   kafka-topics - CLI tool for Kafka topics management

USAGE:
   kafka-topics [global options] command [command options] [arguments...]

COMMANDS:
   help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --file topics.yaml, -f topics.yaml    File to load: topics.yaml or `data.log`
   --action value, -a value              Action to take: add/del/list/search/clean/consume/produce
   --config config.yaml, -c config.yaml  YAML formatted configuration file: config.yaml
   --topic userEvents, -t userEvents     Topic name to consume: userEvents
   --messages 10, -m 10                  Number of messages to consume: 10 (default: 0)
   --version value, -v value             Version of Commands topics: 16
   --yes, -y                             Confirmation for destructive actions (default: false)
   --destroy, -d                         Destroy filtered topics (default: false)
   --help, -h                            show help (default: false)
```
