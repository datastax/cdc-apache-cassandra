# Project: datastax-cdc (cassandra-source-connector)

## What it is

A Change Data Capture (CDC) pipeline that reads Cassandra commit logs and streams mutation events to a messaging system. Both Apache Pulsar and Apache Kafka are supported as messaging backends.

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

### Read Consistency & Row Consistency Guarantees

When reading back the full row upon receiving an event notification:
- The connector executes CQL read queries at `ConsistencyLevel.LOCAL_QUORUM`.
- Downgrade to `ConsistencyLevel.LOCAL_ONE` upon unavailability errors was intentionally removed to avoid silent data inconsistency (reading stale replicas).
- Transient replica unavailability is handled at the connector layer via jittered backoff retries (`SourceUtil.backoffRetry` / `waitForCqlWithRetry`) without re-consuming event topic messages until quorum can be achieved.

## Module map

| Module | Role |
|--------|------|
| `commons` | Shared types: `MutationValue`, `Constants`, `CqlLogicalTypes`, `NativeSchemaWrapper`, `Murmur3MessageRouter` |
| `agent` | Abstract base: `AgentConfig`, `MutationSender<T>`, `AbstractPulsarMutationSender<T>`, `CommitLogReaderService`, `CommitLogProcessor`, `AbstractMutation` |
| `agent-c3` | Cassandra 3 concrete implementation: `PulsarMutationSender`, `Agent`, `CommitLogReaderServiceImpl` |
| `agent-c4` | Cassandra 4 concrete implementation: same structure as agent-c3 |
| `agent-dse4` | DSE 4 variant, opt-in via Gradle flag `dse4` |
| `connector` | Pulsar IO source connector (`CassandraSource`) + Kafka Connect source connector (`KafkaCassandraSourceConnector`, `KafkaCassandraSourceTask`), shared `CassandraSourceConnectorConfig`, converters |
| `connector-distribution` | NAR packaging for Pulsar IO |
| `backfill-cli` | CLI tool to replay historical data through the CDC pipeline. Supports both Pulsar and Kafka via `--platform`. |
| `testcontainers` | Shared test utilities, testcontainers wrappers for Pulsar, Kafka, and Cassandra |

## Key classes

- [`AgentConfig`](../../agent/src/main/java/com/datastax/oss/cdc/agent/AgentConfig.java) — All config settings, with `Platform` enum (`ALL`, `PULSAR`). Platform-specific settings are guarded. Config can be loaded from a `.properties` file (`configFile` setting) and overridden by agent args. Keys may carry a `pulsar.` or `kafka.` prefix to scope them to a specific platform.
- `MutationSenderAvroUtil` *(new, `agent` module)* — Static utility with zero Pulsar/Kafka dependencies. Holds the shared Avro logic extracted from `AbstractPulsarMutationSender`: logical-type conversion registration, `SchemaAndWriter`, `serializeAvroGenericRecord`, `getAvroKeySchema`, `buildAvroKey`.
- [`AbstractPulsarMutationSender`](../../agent/src/main/java/com/datastax/oss/cdc/agent/AbstractPulsarMutationSender.java) — Pulsar-specific: initialises `PulsarClient`, manages `Producer` per topic, calls `MutationSenderAvroUtil` for key serialisation, sends mutations asynchronously.
- `AbstractKafkaMutationSender` *(new, `agent` module)* — Kafka-specific mirror: initialises `KafkaProducer`, manages one producer per topic, calls `MutationSenderAvroUtil` for key serialisation, sends via `producer.send` returning a `CompletableFuture`.
- [`MutationSender`](../../agent/src/main/java/com/datastax/oss/cdc/agent/MutationSender.java) — Interface with `initialize(AgentConfig)` and `sendMutationAsync(AbstractMutation<T>)`.
- [`CassandraSource`](../../connector/src/main/java/com/datastax/oss/pulsar/source/CassandraSource.java) — Pulsar IO source connector. Subscribes to events topic, queries Cassandra, publishes to data topic.
- [`CassandraSourceConnectorConfig`](../../connector/src/main/java/com/datastax/oss/cdc/CassandraSourceConnectorConfig.java) — Config used by both the Pulsar and Kafka connectors (Kafka `ConfigDef` / `AbstractConfig`). Includes schema registry settings (`schema.registry.url`, `schema.registry.autoRegisterSchemas`, basic auth).
- `KafkaCassandraSourceTask` — Kafka Connect source task. Uses BookKeeper `OrderedExecutor` (fixed thread pool, per-key ordering) for CQL read-back. Optionally publishes values in Confluent wire format when `schema.registry.url` is set, via `KafkaAvroSerializer` from `io.confluent:kafka-avro-serializer`.
- `KafkaAvroConverter` — Extends `AvroRowConverter`. When schema registry is enabled, serializes via `KafkaAvroSerializer` (Confluent wire format: magic byte + schema ID + Avro bytes). When disabled, serializes as raw Avro bytes.

## Message format (events topic)

Each mutation is published with an Avro-encoded primary key, a change descriptor (`MutationValue`: md5 digest, node ID), and metadata (segment position, token, write time). The encoding is identical between Pulsar and Kafka backends.

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

## Completed goals

1. ✅ Shared Avro logic extracted into `MutationSenderAvroUtil` — used by both Pulsar and Kafka senders.
2. ✅ `configFile` / `kafkaConfigFile` loading in `AgentConfig` — settings manageable via a properties file.
3. ✅ `pulsar.` / `kafka.` prefix scoping in config files.
4. ✅ Kafka producer support in agent (`AbstractKafkaMutationSender` / `KafkaMutationSender`).
5. ✅ Kafka Connect source connector (`KafkaCassandraSourceConnector` / `KafkaCassandraSourceTask`) in the `connector` module.
6. ✅ All existing Pulsar code intact and backward compatible.
7. ✅ `AgentConfig.Platform.KAFKA` added.
8. ✅ Schema Registry support via `io.confluent:kafka-avro-serializer` — opt-in, compatible with any Confluent Schema Registry API-compatible registry.
9. ✅ `AdaptiveQueryExecutor` removed — replaced by BookKeeper `OrderedExecutor`.
10. ✅ Backfill CLI extended with `--platform KAFKA` and `--kafka-config-file`.

## Key stakeholders / users

- DataStax engineering team
- Cassandra operators who need to stream row-level changes to Kafka consumers
