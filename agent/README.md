# CDC replication common module

## Overview

The agent module provides the core CDC (Change Data Capture) functionality for Apache Cassandra. It supports multiple messaging platforms through a unified abstraction layer.

## Supported Messaging Platforms

- **Apache Pulsar** (2.8.1+) - Default
- **Apache Kafka** (2.8+, 3.x) - Available

## Configuration

### Pulsar Configuration (Default)

```properties
messagingProvider=PULSAR
pulsarServiceUrl=pulsar://localhost:6650
```

### Kafka Configuration

```properties
messagingProvider=KAFKA
kafkaBootstrapServers=localhost:9092
kafkaAcks=all
kafkaCompressionType=snappy
kafkaBatchSize=16384
kafkaLingerMs=10
kafkaMaxInFlightRequests=5
kafkaSchemaRegistryUrl=http://localhost:8081
```

## Build

    ./gradlew agent:jar
    ./gradlew agent:publishToMavenLocal

## Dependencies

- messaging-api: Core messaging abstractions
- messaging-pulsar: Pulsar implementation
- messaging-kafka: Kafka implementation (via SPI)
