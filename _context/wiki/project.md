# Project: datastax-cdc (cassandra-source-connector)

## What it is

A Change Data Capture (CDC) pipeline that reads Cassandra commit logs and streams mutation events to a messaging system. Currently Pulsar is the only supported messaging backend. The active work (branch `feat/kafka-support`) is adding Kafka support without breaking Pulsar.

## How it works (data flow)

```
Cassandra node
  └─ CDC commit log files
       └─ Agent (JVM agent, attaches to Cassandra process)
            └─ CommitLogReaderService  ──reads──►  AbstractMutation
                 └─ MutationSender  ──publishes──►  Pulsar/Kafka topic  (events topic)
                                                           │
                                              Connector (Pulsar Source or Kafka Source Connector)
                                                           │
                                              Reads event, queries Cassandra via CQL
                                                           │
                                              Publishes full row  ──►  data topic / Kafka topic
```

## Module map

| Module | Role |
|--------|------|
| `commons` | Shared types: `MutationValue`, `Constants`, `CqlLogicalTypes`, `NativeSchemaWrapper`, `Murmur3MessageRouter` |
| `agent` | Abstract base: `AgentConfig`, `MutationSender<T>`, `AbstractPulsarMutationSender<T>`, `CommitLogReaderService`, `CommitLogProcessor`, `AbstractMutation` |
| `agent-c3` | Cassandra 3 concrete implementation: `PulsarMutationSender`, `Agent`, `CommitLogReaderServiceImpl` |
| `agent-c4` | Cassandra 4 concrete implementation: same structure as agent-c3 |
| `agent-dse4` | DSE 4 variant, opt-in via Gradle flag `dse4` |
| `connector` | Pulsar IO source connector: `CassandraSource`, `CassandraSourceConnectorConfig`, converters |
| `connector-distribution` | NAR packaging for Pulsar IO |
| `testcontainers` | Shared test utilities, testcontainers wrappers for Pulsar and Cassandra |

## Key classes

- [`AgentConfig`](../../agent/src/main/java/com/datastax/oss/cdc/agent/AgentConfig.java) — All config settings, with `Platform` enum (`ALL`, `PULSAR`). Platform-specific settings are guarded. Config can be loaded from a `.properties` file (`configFile` setting) and overridden by agent args. Keys may carry a `pulsar.` or `kafka.` prefix to scope them to a specific platform.
- `MutationSenderAvroUtil` *(new, `agent` module)* — Static utility with zero Pulsar/Kafka dependencies. Holds the shared Avro logic extracted from `AbstractPulsarMutationSender`: logical-type conversion registration, `SchemaAndWriter`, `serializeAvroGenericRecord`, `getAvroKeySchema`, `buildAvroKey`.
- [`AbstractPulsarMutationSender`](../../agent/src/main/java/com/datastax/oss/cdc/agent/AbstractPulsarMutationSender.java) — Pulsar-specific: initialises `PulsarClient`, manages `Producer` per topic, calls `MutationSenderAvroUtil` for key serialisation, sends mutations asynchronously.
- `AbstractKafkaMutationSender` *(new, `agent` module)* — Kafka-specific mirror: initialises `KafkaProducer`, manages one producer per topic, calls `MutationSenderAvroUtil` for key serialisation, sends via `producer.send` returning a `CompletableFuture`.
- [`MutationSender`](../../agent/src/main/java/com/datastax/oss/cdc/agent/MutationSender.java) — Interface with `initialize(AgentConfig)` and `sendMutationAsync(AbstractMutation<T>)`.
- [`CassandraSource`](../../connector/src/main/java/com/datastax/oss/pulsar/source/CassandraSource.java) — Pulsar IO source connector. Subscribes to events topic, queries Cassandra, publishes to data topic.
- [`CassandraSourceConnectorConfig`](../../connector/src/main/java/com/datastax/oss/cdc/CassandraSourceConnectorConfig.java) — Config used by both the Pulsar connector and the Kafka connector side (already uses Kafka's `ConfigDef` / `AbstractConfig`).

## Message format (events topic)

Each mutation is published as a Pulsar `KeyValue<byte[], MutationValue>` message with:
- **Key**: Avro-serialised primary key (binary)
- **Value**: `MutationValue` (Avro) — `md5Digest`, `nodeId`, `columns[]`
- **Properties**: `segpos` (segment:position), `token`, optionally `writetime`

For the Kafka events topic the same Avro encoding is used (via `MutationSenderAvroUtil`); properties become Kafka record headers.

## AgentConfig loading priority (lowest → highest)

1. Defaults (field initialisers / env-var defaults in each `Setting`)
2. `.properties` file named by the `configFile` agent arg
3. Agent args string (comma-separated `key=value` pairs passed to `-javaagent`)

### Platform-prefix scoping

Any key in the config file **or** the agent args string may carry a `pulsar.` or `kafka.` prefix:

```
# cdc.properties — shared by both platforms
topicPrefix=events-
pulsar.pulsarServiceUrl=pulsar://broker:6650
kafka.kafkaBootstrapServers=broker:9092
```

- A `pulsar.` prefixed key is applied **only** when `platform == PULSAR`; silently skipped for Kafka.
- A `kafka.` prefixed key is applied **only** when `platform == KAFKA`; silently skipped for Pulsar.
- Un-prefixed keys are applied to whichever platform is active (existing behaviour, validated against `setting.platform`).

## Goals

1. Extract shared Avro logic from `AbstractPulsarMutationSender` into `MutationSenderAvroUtil` so both Pulsar and Kafka senders share one implementation.
2. Add `configFile` loading to `AgentConfig` so operators can manage settings in a file instead of a long agent args string.
3. Add `pulsar.` / `kafka.` prefix scoping so a single config file can carry both platforms' settings without conflicts.
4. Add Kafka producer support to the agent (new `AbstractKafkaMutationSender` / `KafkaMutationSender`) so mutations can be sent to Kafka topics instead of Pulsar.
5. Add a Kafka Connect source connector (new module) that reads from the Kafka events topic and publishes full rows.
6. Keep all existing Pulsar code fully intact and backward compatible.
7. Extend `AgentConfig.Platform` to include `KAFKA` and tag new settings accordingly.

## Key stakeholders / users

- DataStax engineering team
- Cassandra operators who need to stream row-level changes to Kafka consumers
