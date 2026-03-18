# DSE CDC agent for Apache Pulsar and Apache Kafka

## Overview

CDC agent for DataStax Enterprise 4.x with support for both Apache Pulsar and Apache Kafka.

## Build

    ./gradlew agent-dse4:shadowJar

## Run with Pulsar (Default)

    export JVM_EXTRA_OPTS="-javaagent:agent-dse4/build/libs/agent-dse4-<version>-all.jar=pulsarServiceUrl=pulsar://pulsar:6650,cdcWorkingDir=/var/lib/cassandra/cdc"

## Run with Kafka

    export JVM_EXTRA_OPTS="-javaagent:agent-dse4/build/libs/agent-dse4-<version>-all.jar=messagingProvider=KAFKA,kafkaBootstrapServers=localhost:9092,cdcWorkingDir=/var/lib/cassandra/cdc"

## Configuration

See [agent/README.md](../agent/README.md) for full configuration options.


