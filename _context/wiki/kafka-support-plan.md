# Kafka Support — Planning Document

**Branch**: `feat/kafka-support`  
**Status**: Draft

---

## 1. Motivation

The CDC pipeline currently hard-wires Pulsar as the only messaging backend.  
Adding Kafka support broadens adoption significantly, since Kafka is the dominant enterprise messaging system.  
**Constraint**: all existing Pulsar behaviour must remain unchanged; no Pulsar class may be modified unless strictly necessary for the abstraction split.

---

## 2. Architecture overview

The pipeline has two independently deployable halves:

```
[ Cassandra node ]
      │  (commit log)
      ▼
[ CDC Agent (JVM agent) ]  ──►  events topic  ──►  [ Source Connector ]  ──►  data topic
```

Both halves need a Kafka variant:

| Half | Current (Pulsar) | New (Kafka) |
|------|-----------------|-------------|
| Agent sender | `AbstractPulsarMutationSender` / `PulsarMutationSender` | `AbstractKafkaMutationSender` / `KafkaMutationSender` |
| Source connector | `CassandraSource` (Pulsar IO) | `KafkaCassandraSourceTask` + `KafkaCassandraSourceConnector` (Kafka Connect) |

---

## 3. Agent-side changes

### 3.1 New `AgentConfig.Platform` value

```java
public enum Platform {
    ALL, PULSAR, KAFKA
}
```

New Kafka-specific settings follow the same `Setting<T>` pattern as existing Pulsar settings, grouped under `"kafka"`:

| Setting name | Type | Default | Description |
|-------------|------|---------|-------------|
| `kafkaBootstrapServers` | String | `localhost:9092` | Kafka broker list |
| `kafkaBatchDelayInMs` | Long | `-1` | Linger.ms equivalent; ≤0 = disabled |
| `kafkaMaxPendingMessages` | Integer | `1000` | Max in-flight send futures before back-pressure |
| `kafkaSecurityProtocol` | String | `PLAINTEXT` | `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL` |
| `kafkaSslKeystoreLocation` | String | null | For SSL transport |
| `kafkaSslKeystorePassword` | String | null | |
| `kafkaSslTruststoreLocation` | String | null | |
| `kafkaSslTruststorePassword` | String | null | |
| `kafkaSaslMechanism` | String | null | e.g. `PLAIN`, `SCRAM-SHA-256` |
| `kafkaSaslJaasConfig` | String | null | Full JAAS config string |

### 3.2 `AbstractKafkaMutationSender<T>` (in `agent` module)

Mirrors `AbstractPulsarMutationSender<T>`:

- Implements `MutationSender<T>`
- Uses Kafka `KafkaProducer<byte[], byte[]>` (raw bytes — Avro key serialised the same way as today, value = Avro-serialised `MutationValue`)
- Caches one `KafkaProducer` per topic (same `producers` map pattern)
- Sends via `producer.send(record, callback)` returning a `CompletableFuture`
- Message headers carry `segpos`, `token`, `writetime` (mirroring Pulsar message properties in `Constants`)
- Murmur3 partitioner: implement a custom `Partitioner` that reuses `Murmur3MessageRouter` logic

**Key design decision**: The Avro serialisation of the primary key (`serializeAvroGenericRecord`) is **shared** — reuse `AbstractPulsarMutationSender.serializeAvroGenericRecord` by extracting it to a utility class or keeping it in `AbstractPulsarMutationSender` and referencing it from `AbstractKafkaMutationSender`. Prefer a small shared utility in `commons` or `agent` module.

### 3.3 Concrete `KafkaMutationSender` (one per agent-c3, agent-c4, agent-dse4)

Mirrors `PulsarMutationSender` in each agent:

- Extends `AbstractKafkaMutationSender<TableMetadata>`
- Same `avroSchemaTypes` map and `cqlToAvro` logic (can be extracted to a shared helper)
- Same `isSupported` check
- `getHostId()` from `StorageService`
- `incSkippedMutations()` via `CdcMetrics`

### 3.4 `Agent.java` changes (agent-c3, agent-c4, agent-dse4)

Add a branch in `startCdcAgent`:

```java
AgentConfig config = AgentConfig.create(platform, agentArgs);
MutationSender<?> sender;
if (platform == AgentConfig.Platform.KAFKA) {
    sender = new KafkaMutationSender(config);
} else {
    sender = new PulsarMutationSender(config);
}
```

Platform detection: read a new `CDC_PLATFORM` env var or a `platform` agent parameter defaulting to `PULSAR`.

---

## 4. Connector-side changes

### 4.1 New Gradle module: `kafka-connector`

Mirrors the `connector` module (which is the Pulsar source).

```
kafka-connector/
  build.gradle
  src/main/java/com/datastax/oss/kafka/source/
    KafkaCassandraSourceConnector.java   # implements Connector
    KafkaCassandraSourceTask.java        # implements SourceTask
    KafkaCassandraSourceConfig.java      # wraps CassandraSourceConnectorConfig
  src/test/...
```

#### Configuration

`KafkaCassandraSourceConfig` wraps `CassandraSourceConnectorConfig`.  
Pulsar-only fields (`events.subscription.name`, `events.subscription.type`) are **not** exposed.  
New Kafka-specific fields:

| Property | Default | Description |
|----------|---------|-------------|
| `events.topic` | (required) | Same as Pulsar — the Kafka topic to consume events from |
| `events.group.id` | `cdc-source` | Kafka consumer group ID |
| `events.auto.offset.reset` | `earliest` | Consumer offset reset policy |

#### `KafkaCassandraSourceTask`

Mirrors `CassandraSource` (Pulsar):

- Holds a `KafkaConsumer<byte[], byte[]>` for the events topic
- Deserialises the Avro key (shared `AvroRowConverter` path, extracted from Pulsar's `NativeSchemaWrapper` handling into `com.datastax.oss.cdc.converters`)
- Queries Cassandra via `CassandraClient` (unchanged, already in `connector` module)
- Publishes `SourceRecord` objects to the Kafka Connect framework
- Reuses `MutationCache`, `ConverterAndQuery`, `Converter` implementations from `connector`

### 4.2 Output format

Kafka Connect tasks produce `SourceRecord` with:
- **Key**: byte[] Avro-encoded primary key (same Avro schema as agent)
- **Value**: byte[] Avro or JSON-encoded row (driven by `outputFormat` config, same as Pulsar)
- **Headers**: `writetime` if present

### 4.3 Offset tracking

Kafka Connect manages offsets natively via `SourceTaskContext.offsetStorageReader`.  
The offset map key: `{"topic": "<events-topic>", "partition": <partition>}`.  
Value: `{"offset": <kafka-offset>}`.

---

## 5. Module changes summary

| Module | Change |
|--------|--------|
| `commons` | Add Avro serialisation utility extracted from `AbstractPulsarMutationSender` (optional, avoids duplication) |
| `agent` | Add `AbstractKafkaMutationSender`, add `KAFKA` to `Platform` enum, add Kafka `Setting<>` entries in `AgentConfig` |
| `agent-c3` | Add `KafkaMutationSender`, update `Agent.java` |
| `agent-c4` | Add `KafkaMutationSender`, update `Agent.java` |
| `agent-dse4` | Add `KafkaMutationSender`, update `Agent.java` |
| `connector` | No change (Pulsar source, untouched) |
| `kafka-connector` *(new)* | `KafkaCassandraSourceConnector`, `KafkaCassandraSourceTask`, `KafkaCassandraSourceConfig` |
| `settings.gradle` | Add `include 'kafka-connector'` |
| `testcontainers` | Add `KafkaContainer` wrapper, add `KafkaSingleNodeTests` / `KafkaDualNodeTests` base classes |

---

## 6. Backward compatibility guarantees

- `AgentConfig.Platform.PULSAR` and all `Platform.PULSAR` settings remain exactly as-is
- Default platform when no `platform` agent arg is supplied stays `PULSAR`
- `AbstractPulsarMutationSender`, `PulsarMutationSender`, `CassandraSource`, `CassandraSourceConnectorConfig` — **zero changes**
- The Avro message format on the events topic is identical between Pulsar and Kafka backends (same schema, same binary encoding) so a Cassandra cluster could theoretically serve both connector types simultaneously

---

## 7. Build dependency decisions

- Kafka producer in `agent` module: add `org.apache.kafka:kafka-clients:${kafkaVersion}` as `implementation` (not `compileOnly`)
- Kafka Connect API in `kafka-connector`: `org.apache.kafka:connect-api:${kafkaVersion}` as `compileOnly` (provided by Connect runtime)
- `kafkaVersion` property already exists in `gradle.properties` (used by `connector` for `connect-api`)

---

## 8. Testing strategy

| Layer | Approach |
|-------|----------|
| Unit | `AgentConfig` Kafka settings parsing (mirrors `AgentParametersTest`) |
| Unit | `AbstractKafkaMutationSender` Avro key serialisation |
| Integration | `KafkaSingleNodeC4Tests` using testcontainers (Kafka + Cassandra) |
| Integration | `KafkaCassandraSourceTask` reading from Kafka events topic, verifying data topic |
| Regression | Existing Pulsar tests must remain green, no modifications |

---

## 9. Open questions

1. **Schema Registry**: Should the Kafka connector optionally support Confluent Schema Registry for Avro schemas on the data topic? Start without it; add as a follow-up.
2. **Exactly-once semantics**: Kafka Connect idempotent producers + transactional consumers? Defer to follow-up; start with at-least-once (same as Pulsar today).
3. **Topic naming**: Pulsar uses `topicPrefix` + `keyspace.table`. Kafka topic names cannot contain `.` safely in all environments. Should we replace `.` with `_` or make the separator configurable? **Proposed default**: replace `.` with `-` and document it.
4. **agent-dse4**: Is the DSE4 agent in scope for this first Kafka pass?
5. **NAR vs JAR**: The Pulsar connector is packaged as a `.nar`. The Kafka connector should be packaged as a fat JAR (shadow jar). Confirm packaging requirements.

---

## 10. Suggested implementation order

1. `AgentConfig`: add `KAFKA` platform + Kafka settings
2. `agent`: add `AbstractKafkaMutationSender`
3. `agent-c4`: add `KafkaMutationSender` + update `Agent.java`
4. `agent-c3`: same
5. `kafka-connector` module: scaffold + `KafkaCassandraSourceConnector` + `KafkaCassandraSourceTask`
6. `testcontainers`: add Kafka test utilities
7. `agent-c4` integration tests with Kafka
8. `connector` Pulsar regression test run
