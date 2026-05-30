# Kafka / Confluent Support

This project historically streamed Cassandra CDC mutations only to Apache Pulsar. It now ships a
**provider-agnostic messaging abstraction** (`messaging-api`, with `messaging-pulsar` and
`messaging-kafka` implementations) so the **CDC agent can publish change events to either Apache
Pulsar or Apache Kafka / Confluent**, selected at runtime — with no change to the existing Pulsar
behaviour or wire format.

## Architecture

```
Cassandra node ──► CDC agent ──► MessagingClient (SPI)
                                   ├─ PulsarMessagingClient ──► Pulsar events topic
                                   └─ KafkaMessagingClient  ──► Kafka  events topic
```

The provider is chosen by the `messagingProvider` agent parameter. The messaging client is
discovered via Java's `ServiceLoader` (`META-INF/services/...MessagingClientProvider`), so the agent
jar simply needs both provider modules on its classpath (they already are).

## Enabling Kafka on the agent

Add the agent as a `-javaagent` with `messagingProvider=kafka` and the Kafka bootstrap servers:

```
-javaagent:/path/agent-c4-<version>-all.jar=messagingProvider=kafka,kafkaBootstrapServers=broker1:9092\,broker2:9092
```

### Agent Kafka parameters

| Parameter | Env var | Default | Description |
|-----------|---------|---------|-------------|
| `messagingProvider` | `CDC_MESSAGING_PROVIDER` | `pulsar` | `pulsar` or `kafka`. |
| `kafkaBootstrapServers` | `CDC_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka bootstrap servers. |
| `kafkaAcks` | `CDC_KAFKA_ACKS` | `all` | Producer `acks`. |
| `kafkaCompressionType` | `CDC_KAFKA_COMPRESSION_TYPE` | `none` | `none`/`gzip`/`snappy`/`lz4`/`zstd`. |
| `kafkaBatchSize` | `CDC_KAFKA_BATCH_SIZE` | `16384` | Producer `batch.size`. |
| `kafkaLingerMs` | `CDC_KAFKA_LINGER_MS` | `0` | Producer `linger.ms`. |
| `kafkaMaxInFlightRequests` | `CDC_KAFKA_MAX_IN_FLIGHT_REQUESTS` | `5` | `max.in.flight.requests.per.connection`. |
| `kafkaSchemaRegistryUrl` | `CDC_KAFKA_SCHEMA_REGISTRY_URL` | _(unset)_ | Confluent Schema Registry URL — see serialization below. |

SSL/TLS parameters (`sslKeystorePath`, `sslTruststorePath`, `useKeyStoreTls`, …) are shared with the
Pulsar configuration and are mapped to the equivalent Kafka client SSL settings.

The Pulsar parameters (`pulsarServiceUrl`, `pulsarBatchDelayInMs`, …) continue to work unchanged when
`messagingProvider=pulsar` (the default).

## Serialization (configurable)

The agent publishes each event as `key = <primary key>`, `value = MutationValue`, with the
`writetime`, `segpos` and `token` carried as Kafka record headers. Two serialization modes are
supported and selected automatically:

- **Registry-less (default)** — when no `kafkaSchemaRegistryUrl` is set. The primary key and
  `MutationValue` are encoded as **raw Avro binary**. This works against plain Apache Kafka with no
  Schema Registry. Consumers decode the value with the canonical `MutationValue` Avro schema
  (`com.datastax.oss.cdc.MutationValueCodec`).
- **Confluent Schema Registry** — when `kafkaSchemaRegistryUrl` is set. The key and value are
  serialized with `KafkaAvroSerializer` and schemas are auto-registered under
  `<topic>-key` / `<topic>-value`.

> Note: the registry-less path is the fully exercised default. With the Schema Registry path,
> primary keys that use the custom CQL logical types `varint`/`decimal` are a known limitation
> (standard types, including `uuid`, are handled).

## Topic naming

Identical to the Pulsar convention: events are published to `${topicPrefix}<keyspace>.<table>`
(default prefix `events-`), e.g. `events-myks.users`.

## Testing & CI

- Agent → Kafka is covered by `KafkaSingleNodeC4Tests` (Testcontainers, `confluentinc/cp-kafka`).
  These tests are tagged `@Tag("kafka")` and run in the dedicated `test-kafka` CI job (or locally
  with `-PkafkaTests`); they are excluded from the default/Pulsar test runs.
- The messaging modules have unit tests (`MutationValueCodecTest`, `RawAvroSerdeTest`,
  `KafkaMessagingClientTest`, provider SPI discovery, `ProducerConfigBuilderTest`).

### Local integration testing note

The Testcontainers docker-java client defaults to Docker Engine API `1.32`, which newer Docker
engines reject. Pass `-Papi.version=1.43` to force a supported version (CI does this). On Docker
Desktop you may also need `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock`.

## Status: source connector (events → Cassandra → data)

The **agent (producer) side is complete for both providers.** The **source connector** — which
consumes the events topic, queries Cassandra for the current row, and publishes to the `data-*`
topic — currently exists only for Pulsar (it runs inside the Pulsar IO runtime; see
`docs/CONNECTOR_ARCHITECTURE_DECISION.md`). A Kafka equivalent is a separate Kafka Connect plugin and
is tracked as follow-up work.
