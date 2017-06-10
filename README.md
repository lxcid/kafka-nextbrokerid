# Kafka's `nextbrokerid`

## Introduction

This is a small utility Java program that connects to [Zookeeper](https://zookeeper.apache.org) to figure out the next broker ID available. We make several assumptions:

- Zookeeper keeps the currently used IDs by Kafka in `/brokers/ids`.
- Possible IDs are sequential from `start` to `start + total`.
- Smallest ID available are always return first.

It replicates the algorithm described in [Alex Etling's post on Scaling with Kafka](http://tech.gc.com/scaling-with-kafka/).

This utility is very useful for scaling Kafka in a containerized environment.

## Usage

Downloads from latest release (v1.0.0).

```
# wget
wget \
  -q https://github.com/lxcid/kafka-nextbrokerid/releases/download/v1.0.0/nextbrokerid.jar \
  -O /tmp/nextbrokerid.jar
```

Get next broker ID.

```sh
java -jar nextbrokerid.jar -zkc localhost:2181 --start 1 --total 100 --timeout 30000
```

## References

- http://tech.gc.com/scaling-with-kafka/
- https://stackoverflow.com/a/367714
- https://stackoverflow.com/a/33603040
