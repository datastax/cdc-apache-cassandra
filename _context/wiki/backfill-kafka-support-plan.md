# Backfill CLI — Kafka Support Design Plan

**Branch**: `feat/kafka-support`
**Status**: Approved, pending implementation

---

## 1. Context

The backfill CLI today runs the pipeline:

```
Cassandra  →  disk (DSBulk unload)  →  PulsarImporter  →  Pulsar events topic
```

This document describes the minimal changes needed to add a parallel Kafka path:

```
Cassandra  →  disk (DSBulk unload)  →  KafkaImporter  →  Kafka events topic
```

The export half (DSBulk, `TableExporter`, `ExportSettings`) is **unchanged** — it is messaging-backend-agnostic. Only the import half changes.

---

## 2. Key design decision — no `KafkaImportSettings` class

`AbstractKafkaMutationSender.initialize()` reads broker-level properties (`bootstrapServers`, SSL, SASL, etc.) from `AgentConfig` via `config.get(key)`. These keys are **not** registered `Setting<>` fields — they are loaded from a kafka config file into `AgentConfig`'s raw properties map.

Therefore the factory only needs two inputs:
- `kafkaConfigFile` — path to a standard Kafka producer `.properties` file
- `topicPrefix` — already present in `ImportSettings`

There is no need for individual CLI flags per Kafka property. All broker-level configuration is delegated to the config file, which `AgentConfig.configure(KAFKA, …)` loads automatically.

---

## 3. Shared base class — `AbstractImporter`

`PulsarImporter` and `KafkaImporter` share ~150 lines of identical logic:
- CSV reading + PK codec preparation
- `createMutation()` (constants are identical for both)
- `sendMutationAsync()` (acquire semaphore → call `mutationSender.sendMutationAsync()` → handle → release)
- `printSummary()`

Rather than duplicating this, a package-private `AbstractImporter` is extracted in `importer/`. Both senders implement `MutationSender<TableMetadata>` and `AutoCloseable`, so the base class holds `MutationSender<TableMetadata>` and casts to `AutoCloseable` for `close()`.

```
AbstractImporter   (new, package-private)
  - MutationSender<TableMetadata> mutationSender
  - importTable(), createMutation(), sendMutationAsync(), printSummary()
  - abstract String importerName()   ← "Pulsar" or "Kafka" for log lines

PulsarImporter extends AbstractImporter   ← refactored, public API unchanged
KafkaImporter  extends AbstractImporter   ← new, trivial constructor only
```

`PulsarImporter`'s constructor signature is **unchanged** — `PulsarImporterTest` and `BackfillFactory` require no modifications to their call sites.

---

## 4. Files to create

### 4.1 `factory/KafkaMutationSenderFactory.java` (new)

Mirrors `PulsarMutationSenderFactory`. Takes `kafkaConfigFile` and `topicPrefix`, calls:

```java
AgentConfig config = AgentConfig.create(Platform.KAFKA,
    Map.of("kafkaConfigFile", kafkaConfigFile, "topicPrefix", topicPrefix));
return new KafkaMutationSender(config, false); // false = no Murmur3, round-robin
```

`false` for `useMurmur3Partitioner` matches the Pulsar importer behaviour — backfill uses round-robin routing, not token-aware partitioning.

### 4.2 `importer/AbstractImporter.java` (new, package-private)

Contains all shared import logic. Subclasses only provide `importerName()` and a constructor.

### 4.3 `importer/KafkaImporter.java` (new)

Extends `AbstractImporter`. Constructor accepts `KafkaMutationSenderFactory`, passes `"Kafka"` as the importer name. No other logic.

---

## 5. Files to modify

### 5.1 `importer/ImportSettings.java`

Add one new option alongside the existing Pulsar options:

```java
@CommandLine.Option(
    names = "--kafka-config-file",
    description = "Path to a Kafka producer properties file (bootstrapServers, security settings, etc.).")
public String kafkaConfigFile;
```

The existing `--events-topic-prefix` option is shared between both backends — no duplication needed.

### 5.2 `importer/PulsarImporter.java`

Refactored to extend `AbstractImporter`. The public constructor signature is **unchanged**. All logic moves to the base class; `PulsarImporter` only provides `importerName()` returning `"Pulsar"`.

### 5.3 `BackfillSettings.java`

Add a `--platform` option:

```java
@CommandLine.Option(
    names = "--platform",
    description = "The messaging platform to import to: PULSAR or KAFKA. Default: PULSAR.",
    defaultValue = "PULSAR")
public AgentConfig.Platform platform = AgentConfig.Platform.PULSAR;
```

### 5.4 `factory/BackfillFactory.java`

Add a factory method:

```java
public KafkaImporter newKafkaImporter(ConnectorFactory connectorFactory, ExportedTable exportedTable) {
    return new KafkaImporter(connectorFactory, exportedTable,
            new KafkaMutationSenderFactory(settings.importSettings));
}
```

### 5.5 `BackfillCLI.java`

Branch on `settings.platform` in `call()`:

```java
if (settings.platform == AgentConfig.Platform.KAFKA) {
    KafkaImporter importer = factory.newKafkaImporter(connectorFactory, exporter.getExportedTable());
    // run kafka migration
} else {
    PulsarImporter importer = factory.newPulsarImporter(connectorFactory, exporter.getExportedTable());
    // existing pulsar migration
}
```

### 5.6 `build.gradle`

Add `kafka-clients` to the `dependencies` block:

```groovy
implementation "org.apache.kafka:kafka-clients:${kafkaVersion}"
```

---

## 6. Testing strategy

Mirrors the existing Pulsar test suite for full parity. The shared logic in `AbstractImporter` is tested once in `AbstractImporterTest`; concrete subclass tests are thin wrappers that confirm wiring only.

### 6.1 Unit tests

| New test class | Mirrors | What it covers |
|---|---|---|
| `AbstractImporterTest` | `PulsarImporterTest` (core logic) | All 4 shared behaviours: partition-key-only CSV, composite PK with all type conversions, semaphore back-pressure (1000 in-flight limit), fail-fast on async error. Uses a minimal `TestImporter extends AbstractImporter` stub with a mock `MutationSender`. |
| `CassandraToKafkaMigratorTest` | `CassandraToPulsarMigratorTest` | Happy path (export OK → import called); export-failed (import skipped) |
| `KafkaImporterTest` | `PulsarImporterTest` | Thin wiring smoke test — confirms `KafkaMutationSenderFactory` is called and the sender is used |

`PulsarImporterTest` is left **unchanged** — it stays as a regression guard for the Pulsar wiring.

### 6.2 E2E tests

| New test class | Mirrors | What it covers |
|---|---|---|
| `BackfillCLIKafkaE2ETests` | `BackfillCLIE2ETests` | Full pipeline: Cassandra → disk → `KafkaImporter` → Kafka events topic |

`BackfillCLIKafkaE2ETests` is scaffolded with `@Disabled` pending the `kafka-connector` module. Once `KafkaCassandraSourceTask` exists it can consume the events topic and produce the data topic — the same role `CassandraSource` plays in the Pulsar E2E. Structure:

1. Start `KafkaContainer` + `CassandraContainer` via testcontainers
2. Insert rows into Cassandra (CDC disabled)
3. Run `backfill --platform KAFKA --kafka-config-file <props>` as a subprocess
4. Use `KafkaConsumer` to poll the events topic and assert the received mutations

---

## 7. Files untouched

| File | Reason |
|---|---|
| `CassandraToPulsarMigrator` | Pulsar path unchanged |
| `PulsarMutationSenderFactory` | Pulsar path unchanged |
| `ExportSettings`, `TableExporter` | Export is backend-agnostic |
| All `agent-*` modules | Already have `KafkaMutationSender` |

---

## 8. Summary of new/modified files

| File | Change |
|---|---|
| `importer/AbstractImporter.java` | **New** (package-private shared base) |
| `importer/PulsarImporter.java` | Refactored to extend `AbstractImporter` |
| `importer/KafkaImporter.java` | **New** |
| `factory/KafkaMutationSenderFactory.java` | **New** |
| `importer/ImportSettings.java` | Add `--kafka-config-file` option |
| `BackfillSettings.java` | Add `--platform` option |
| `factory/BackfillFactory.java` | Add `newKafkaImporter()` |
| `BackfillCLI.java` | Branch on platform |
| `build.gradle` | Add `kafka-clients` dep |
| `AbstractImporterTest.java` | **New** unit test — all 4 shared import behaviours |
| `CassandraToKafkaMigratorTest.java` | **New** unit test — migrator orchestration |
| `KafkaImporterTest.java` | **New** unit test — wiring smoke test |
| `BackfillCLIKafkaE2ETests.java` | **New** E2E skeleton (`@Disabled`) |
