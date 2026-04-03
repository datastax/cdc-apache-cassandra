# CDC for Apache Cassandra - Current Architecture

## Project Overview

**CDC for Apache Cassandra** (version 2.3.7) is an open-source Change Data Capture (CDC) solution that captures mutations from Apache Cassandra/DataStax Enterprise (DSE) databases and streams them to Apache Pulsar topics. The project enables real-time data replication and integration with downstream systems like Elasticsearch, Snowflake, and other data platforms.

### Supported Platforms
- **Cassandra**: 3.11.x and 4.0.x
- **DataStax Enterprise (DSE)**: 6.8.16+
- **Apache Pulsar**: 2.8.1+ and IBM Elite Support for Apache Pulsar (formerly Luna Streaming) 2.8+

## High-Level Architecture

The system consists of three main components:

1. **CDC Agent** - JVM agent deployed on each Cassandra node that reads commit logs
2. **Cassandra Source Connector** - Pulsar source connector that queries Cassandra and publishes to data topics
3. **Backfill CLI** - Tool for historical data migration

```
┌─────────────────────────────────────────────────────────────┐
│                    Cassandra Cluster                         │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Node 1                                               │   │
│  │  ┌────────────┐    ┌──────────────┐                 │   │
│  │  │ Cassandra  │───▶│  CDC Agent   │                 │   │
│  │  │ CommitLog  │    │  (JVM Agent) │                 │   │
│  │  │ (cdc_raw)  │    └──────┬───────┘                 │   │
│  │  └────────────┘           │                          │   │
│  └───────────────────────────┼──────────────────────────┘   │
│                               │                              │
│  ┌───────────────────────────┼──────────────────────────┐   │
│  │  Node 2                   │                          │   │
│  │  ┌────────────┐    ┌──────▼───────┐                 │   │
│  │  │ Cassandra  │───▶│  CDC Agent   │                 │   │
│  │  │ CommitLog  │    │  (JVM Agent) │                 │   │
│  │  │ (cdc_raw)  │    └──────┬───────┘                 │   │
│  │  └────────────┘           │                          │   │
│  └───────────────────────────┼──────────────────────────┘   │
└───────────────────────────────┼──────────────────────────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │   Apache Pulsar       │
                    │                       │
                    │  Events Topics        │
                    │  (per table)          │
                    │  events-ks.table      │
                    └───────────┬───────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │  Cassandra Source     │
                    │  Connector            │
                    │  (Pulsar Source)      │
                    └───────────┬───────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │   Data Topics         │
                    │   (per table)         │
                    │   data-ks.table       │
                    └───────────────────────┘
```

## Module Structure

The project is organized as a multi-module Gradle build:

### Core Modules

#### 1. **commons** (`commons/`)
Shared utilities and data structures used across all modules.

**Key Classes:**
- `Constants.java` - System-wide constants (SEGMENT_AND_POSITION, TOKEN, WRITETIME)
- `CqlLogicalTypes.java` - AVRO logical type conversions for CQL types (varint, decimal, UUID)
- `Murmur3MessageRouter.java` - Consistent hashing for message routing using Murmur3
- `MutationValue.java` - AVRO schema for mutation values containing operation metadata
- `NativeSchemaWrapper.java` - Wrapper for native AVRO schemas

**Purpose:** Provides common data structures and utilities for CDC operations, particularly AVRO schema handling and CQL type conversions.

#### 2. **agent** (`agent/`)
Abstract base implementation for CDC agents across different Cassandra versions.

**Key Classes:**

**Configuration:**
- `AgentConfig.java` - Comprehensive configuration management with 20+ settings
  - Topic prefix configuration
  - CDC working directory management
  - Pulsar connection settings (URL, auth, SSL/TLS)
  - Batching and performance tuning
  - Environment variable support

**Core Processing:**
- `CommitLogProcessor.java` - Watches and processes commit log files from `cdc_raw` directory
  - Polls directory at configurable intervals (default: 60s)
  - Handles both `.log` files (C3) and `_cdc.idx` files (C4/DSE4)
  - Supports error commit log reprocessing
  - Maintains processing order via file sorting

- `CommitLogReaderService.java` - Service interface for reading commit logs
- `AbstractProcessor.java` - Base class for background processing tasks
- `AbstractDirectoryWatcher.java` - File system watcher for commit log directory

**Mutation Handling:**
- `AbstractMutation.java` - Base mutation representation
- `AbstractMutationMaker.java` - Creates mutation objects from commit log entries
- `AbstractPulsarMutationSender.java` - Sends mutations to Pulsar topics
  - Builds AVRO schemas for primary keys
  - Manages Pulsar producers per table
  - Handles batching and message routing
  - Implements de-duplication via MD5 digests

**Data Structures:**
- `TableInfo.java` - Table metadata (keyspace, table name, columns)
- `ColumnInfo.java` - Column metadata (name, type, partition/clustering key info)
- `MutationSender.java` - Interface for sending mutations

**Commit Log Management:**
- `CommitLogTransfer.java` - Interface for commit log file management
- `ArchiveCommitLogTransfer.java` - Archives processed commit logs
- `BlackHoleCommitLogTransfer.java` - Deletes processed commit logs
- `CommitLogUtil.java` - Utilities for commit log file operations

**Offset Tracking:**
- `SegmentOffsetWriter.java` - Interface for tracking processing offsets
- `SegmentOffsetFileWriter.java` - File-based offset persistence
- `SegmentOffsetDummyWriter.java` - No-op implementation for testing

**Exceptions:**
- `CassandraConnectorConfigException` - Configuration errors
- `CassandraConnectorDataException` - Data processing errors
- `CassandraConnectorSchemaException` - Schema-related errors
- `CassandraConnectorTaskException` - Task execution errors

#### 3. **agent-c3** (`agent-c3/`)
Cassandra 3.x specific agent implementation.

**Dependencies:**
- Cassandra 3.11.19
- Inherits from `agent` module

**Specifics:**
- Handles `.log` commit log files
- Compatible with Cassandra 3.11+ CDC implementation

#### 4. **agent-c4** (`agent-c4/`)
Cassandra 4.x specific agent implementation.

**Key Classes:**
- `Agent.java` - Main agent entry point for C4
- `CommitLogReaderServiceImpl.java` - C4-specific commit log reading
- `CommitLogReadHandlerImpl.java` - Handles commit log parsing
- `Mutation.java` - C4-specific mutation representation
- `MutationMaker.java` - Creates mutations from C4 commit logs
- `PulsarMutationSender.java` - C4-specific Pulsar sender

**Dependencies:**
- Cassandra 4.0.4
- Supports near-real-time CDC with `_cdc.idx` files

**Docker Support:**
- Dockerfile for containerized deployment
- JMX Prometheus exporter configuration
- Sample table configuration

**Testing:**
- `PulsarSingleNodeC4Tests.java` - Single node integration tests
- `PulsarDualNodeC4Tests.java` - Multi-node replication tests

#### 5. **agent-dse4** (`agent-dse4/`)
DataStax Enterprise 6.8.16+ specific agent implementation.

**Key Classes:**
- Similar structure to agent-c4
- `CdcMetrics.java` - DSE-specific metrics collection
- Additional DSE-specific optimizations

**Dependencies:**
- DSE 6.8.61
- Requires DSE repository credentials

**Features:**
- Near-real-time CDC support
- DSE-specific commit log format handling

#### 6. **agent-distribution** (`agent-distribution/`)
Packaging module for agent distributions.

**Contents:**
- LICENSE.txt
- README.md
- THIRD-PARTY-NOTICES.txt

**Build Output:**
- Creates distributable agent JAR files
- Includes all dependencies

#### 7. **connector** (`connector/`)
Pulsar source connector that reads from events topics and writes to data topics.

**Key Classes:**

**Main Connector:**
- `CassandraSource.java` - Main Pulsar source connector implementation
  - Subscribes to events topics
  - Queries Cassandra for full row data
  - Publishes to data topics with schema
  - Handles de-duplication and ordering

**Configuration:**
- `CassandraSourceConnectorConfig.java` - Comprehensive connector configuration (850 lines)
  - Cassandra connection settings (contact points, port, DC)
  - Events topic subscription (name, type: Key_Shared/Failover)
  - Batch size and query executors
  - Cache configuration (max digests, capacity, expiration)
  - SSL/TLS settings
  - Authentication configuration
  - Output format (key-value-avro, key-value-json, json)
  - Query execution timeouts and backoff strategies

**Cassandra Client:**
- `CassandraClient.java` - Manages Cassandra driver sessions
- `ConfigUtil.java` - Configuration utilities

**Caching:**
- `MutationCache.java` - In-memory cache for mutation de-duplication
  - Caffeine-based cache implementation
  - Configurable capacity and expiration
  - MD5 digest-based de-duplication
  - Coordinator node matching option

**Converters:**
Base converter classes:
- `Converter.java` - Base converter interface
- `ConverterAndQuery.java` - Pairs converter with CQL query

Generic converters (for custom schemas):
- `AbstractGenericConverter.java` - Base for generic converters
- `AvroConverter.java` - Generic AVRO converter
- `JsonConverter.java` - Generic JSON converter
- `ProtobufConverter.java` - Generic Protobuf converter
- `StringConverter.java` - String-based converter

Native converters (for built-in schemas):
- `AbstractNativeConverter.java` - Base for native converters
  - Handles CQL to AVRO/JSON type mapping
  - Supports all CQL data types including UDTs, tuples, collections
  - Manages schema evolution

- `NativeAvroConverter.java` - Native AVRO format converter
  - Key-value AVRO encoding
  - Separate key and value schemas
  - Full CQL type support with logical types

- `NativeJsonConverter.java` - Native JSON format converter
  - Key-value JSON encoding
  - Human-readable format
  - Schema-aware JSON generation

**Version Management:**
- `Version.java` - Connector version information
- `cassandra-source-version.properties` - Version properties file

**Pulsar Integration:**
- `CassandraSourceConfig.java` - Pulsar-specific configuration
- `META-INF/services/pulsar-io.yaml` - Pulsar IO connector descriptor

**Testing:**
- `CassandraSourceConnectorConfigTest.java` - Configuration tests
- `MutationCacheTests.java` - Cache behavior tests
- `AvroKeyValueCassandraSourceTests.java` - AVRO format tests
- `JsonKeyValueCassandraSourceTests.java` - JSON key-value tests
- `JsonOnlyCassandraSourceTests.java` - JSON-only format tests
- `PulsarCassandraSourceTests.java` - Integration tests

#### 8. **connector-distribution** (`connector-distribution/`)
Packaging module for connector distributions.

**Build Output:**
- NAR (Native Archive) file for Pulsar
- Includes all connector dependencies

#### 9. **backfill-cli** (`backfill-cli/`)
Command-line tool for backfilling historical data.

**Purpose:**
Migrates existing Cassandra data to Pulsar by:
1. Exporting primary keys using DataStax Bulk Loader (DSBulk)
2. Sending primary keys to events topics
3. Triggering connector to fetch and publish full rows

**Key Features:**
- Standalone JAR or Pulsar Admin Extension
- DSBulk integration for efficient export
- Configurable batch processing
- Support for all Cassandra versions

**Configuration Groups:**
1. Cassandra parameters (host, credentials, keyspace, table)
2. Pulsar parameters (URL, auth, topic prefix)
3. DSBulk options (CSV connector settings)

**Build Artifacts:**
- `backfill-cli-<version>-all.jar` - Standalone executable
- `pulsar-cassandra-admin-<version>-nar.nar` - Pulsar admin extension

**Testing:**
- `CassandraToPulsarMigratorTest.java` - Migration logic tests
- `PulsarImporterTest.java` - Pulsar import tests
- `TableExporterTest.java` - Export functionality tests
- `BackfillCLIE2ETests.java` - End-to-end tests

#### 10. **testcontainers** (`testcontainers/`)
Shared test infrastructure using Testcontainers.

**Purpose:**
- Provides reusable test containers for Cassandra and Pulsar
- Supports multiple Cassandra versions (C3, C4, DSE4)
- Configurable Pulsar images

#### 11. **docs** (`docs/`)
Antora-based documentation.

**Structure:**
- `modules/ROOT/pages/` - Documentation pages
  - `index.adoc` - Main documentation
  - `install.adoc` - Installation guide
  - `cdc-concepts.adoc` - CDC concepts
  - `cdc-cassandra-events.adoc` - Event format
  - `monitor.adoc` - Monitoring guide
  - `backfill-cli.adoc` - Backfill CLI documentation
  - `faqs.adoc` - Frequently asked questions

- `modules/ROOT/partials/` - Reusable documentation fragments
  - `agentParams.adoc` - Agent parameters (auto-generated)
  - `cfgCassandraSource.adoc` - Connector configuration
  - `cfgCassandraAuth.adoc` - Authentication configuration
  - `cfgCassandraSSL.adoc` - SSL/TLS configuration

## Data Flow Architecture

### 1. Commit Log Processing (Agent)

```
Cassandra Node
    │
    ├─ Write Operation
    │     │
    │     ▼
    ├─ CommitLog (cdc_raw/)
    │     │
    │     ├─ C3: *.log files
    │     └─ C4/DSE4: *_cdc.idx files
    │
    ▼
CDC Agent (JVM Agent)
    │
    ├─ CommitLogProcessor
    │     │
    │     ├─ AbstractDirectoryWatcher (polls every 60s)
    │     └─ Detects new/modified files
    │
    ├─ CommitLogReaderService
    │     │
    │     ├─ Reads commit log entries
    │     ├─ Parses mutations
    │     └─ Maintains offset tracking
    │
    ├─ MutationMaker
    │     │
    │     ├─ Extracts table metadata
    │     ├─ Builds primary key
    │     └─ Creates MutationValue
    │
    ├─ PulsarMutationSender
    │     │
    │     ├─ MD5 digest calculation (de-duplication)
    │     ├─ AVRO schema generation
    │     ├─ Pulsar producer creation
    │     └─ Message properties:
    │           - SEGMENT_AND_POSITION
    │           - TOKEN (partition token)
    │           - WRITETIME (optional)
    │
    └─ Pulsar Events Topic
          │
          └─ Topic: events-<keyspace>.<table>
                │
                └─ Message:
                      Key: Primary key (AVRO)
                      Value: MutationValue (AVRO)
                      Properties: segment, position, token, writetime
```

### 2. Data Topic Publishing (Connector)

```
Pulsar Events Topic
    │
    ▼
Cassandra Source Connector
    │
    ├─ Event Subscription
    │     │
    │     ├─ Subscription Type: Key_Shared (default)
    │     ├─ Subscription Name: configurable
    │     └─ Batch Size: 200 (default)
    │
    ├─ MutationCache
    │     │
    │     ├─ Check MD5 digest
    │     ├─ De-duplicate replicas
    │     └─ Cache expiration: 60s (default)
    │
    ├─ CassandraClient
    │     │
    │     ├─ Query full row data
    │     ├─ Use primary key from event
    │     ├─ Consistency level: LOCAL_QUORUM
    │     └─ Retry with backoff
    │
    ├─ Converter (NativeAvroConverter/NativeJsonConverter)
    │     │
    │     ├─ Convert CQL types to AVRO/JSON
    │     ├─ Handle collections, UDTs, tuples
    │     ├─ Apply column filtering (regex)
    │     └─ Generate schema
    │
    └─ Pulsar Data Topic
          │
          └─ Topic: data-<keyspace>.<table>
                │
                └─ Message:
                      Key: Primary key (AVRO/JSON)
                      Value: Full row (AVRO/JSON) or null (delete)
                      Schema: Auto-updated in registry
```

### 3. Backfill Process

```
Backfill CLI
    │
    ├─ TableExporter (DSBulk)
    │     │
    │     ├─ Export primary keys to CSV
    │     ├─ Query: SELECT pk_columns FROM table
    │     └─ Output: data-dir/*.csv
    │
    ├─ PulsarImporter
    │     │
    │     ├─ Read CSV files
    │     ├─ Parse primary keys
    │     ├─ Create MutationValue (backfill=true)
    │     └─ Send to events topic
    │
    └─ Events Topic
          │
          └─ Connector processes as normal
                │
                └─ Queries Cassandra for full rows
```

## Key Design Patterns

### 1. De-duplication Strategy

**Problem:** Cassandra replicates writes to multiple nodes, causing duplicate mutations.

**Solution:** Three-level de-duplication:

1. **Agent Level (In-Memory Cache)**
   - MD5 digest of mutation (table + pk + token + timestamp)
   - Cache size: configurable
   - Reduces duplicate events sent to Pulsar

2. **Connector Level (MutationCache)**
   - Caffeine cache with configurable capacity (default: 32,767)
   - Expiration: 60 seconds (default)
   - Coordinator node matching option
   - Prevents duplicate queries to Cassandra

3. **Message Properties**
   - TOKEN property for partition token
   - SEGMENT_AND_POSITION for ordering
   - Enables downstream de-duplication if needed

### 2. Schema Evolution

**AVRO Schema Management:**
- Primary key schema: Generated per table
- Value schema: MutationValue (fixed) or native schema (dynamic)
- Schema registry: Pulsar built-in
- Auto-update: Enabled by default

**Schema Changes:**
- ADD COLUMN: New field added to schema
- DROP COLUMN: Field removed from schema
- RENAME: Not supported (creates new field)
- Type changes: Not supported

### 3. Ordering Guarantees

**Commit Log Level:**
- Files processed in order (sorted by segment ID)
- Mutations within file processed sequentially
- Offset tracking ensures no skips

**Pulsar Level:**
- Key_Shared subscription maintains per-key ordering
- Murmur3 partitioning for consistent routing
- SEGMENT_AND_POSITION property for verification

### 4. Fault Tolerance

**Agent:**
- Offset tracking in working directory
- Restart resumes from last offset
- Error commit logs can be reprocessed
- Commit log archiving or deletion

**Connector:**
- Pulsar subscription cursor tracking
- Automatic reconnection to Cassandra
- Configurable retry with exponential backoff
- Query timeout handling

**Backfill:**
- CSV-based checkpointing
- Resumable from last processed file
- Independent of real-time CDC

## Performance Characteristics

### Agent Performance

**Configuration Parameters:**
- `cdcConcurrentProcessors`: Thread pool size (default: memtable_flush_writers)
- `maxInflightMessagesPerTask`: Max pending messages (default: 16,384)
- `pulsarBatchDelayInMs`: Batching delay (default: disabled)
- `pulsarMaxPendingMessages`: Producer queue size (default: 1,000)

**Throughput:**
- Depends on commit log sync period (default: 2s in C3, 10s in C4)
- Near-real-time in C4/DSE4 with `_cdc.idx` files
- Batching improves throughput at cost of latency

**Resource Usage:**
- Memory: Proportional to in-flight messages
- CPU: Commit log parsing and AVRO serialization
- Disk: Working directory for offsets and archives

### Connector Performance

**Configuration Parameters:**
- `batch.size`: Events processed per batch (default: 200)
- `query.executors`: Concurrent query threads (default: 10)
- `maxConcurrentRequests`: Max Cassandra requests (default: 500)
- `cache.max.capacity`: De-duplication cache size (default: 32,767)

**Adaptive Query Execution:**
- Monitors mobile average latency
- Increases executors if latency < min threshold (10ms)
- Decreases executors if latency > max threshold (100ms)
- Dynamic adjustment based on Cassandra load

**Caching:**
- Reduces redundant Cassandra queries
- Configurable expiration (default: 60s)
- Coordinator matching option for accuracy

## Configuration Management

### Agent Configuration

**Sources (Priority Order):**
1. JVM system properties (`-Dcdc.property=value`)
2. Agent parameters (comma-separated string)
3. Environment variables (`CDC_PROPERTY`)
4. Default values

**Example:**
```
-javaagent:agent-c4-2.3.7.jar=pulsarServiceUrl=pulsar://localhost:6650,topicPrefix=events-
```

**Key Settings:**
- `pulsarServiceUrl`: Pulsar broker URL
- `topicPrefix`: Events topic prefix (default: "events-")
- `cdcWorkingDir`: Working directory for offsets
- `cdcDirPollIntervalMs`: Poll interval (default: 60,000ms)
- SSL/TLS: Full certificate and keystore support
- Authentication: Plugin-based (token, OAuth, etc.)

### Connector Configuration

**Pulsar Source Connector Config:**
```yaml
tenant: public
namespace: default
name: cassandra-source-ks1-table1
topicName: data-ks1.table1
archive: builtin://cassandra-source-2.3.7.nar

configs:
  # Cassandra connection
  contactPoints: "127.0.0.1"
  port: 9042
  loadBalancing.localDc: "dc1"
  
  # Table mapping
  keyspace: "ks1"
  table: "table1"
  
  # Events topic
  events.topic: "events-ks1.table1"
  events.subscription.name: "sub"
  events.subscription.type: "Key_Shared"
  
  # Performance
  batch.size: 200
  query.executors: 10
  
  # Cache
  cache.max.capacity: 32767
  cache.expire.after.ms: 60000
  
  # Output format
  outputFormat: "key-value-avro"
```

## Monitoring and Observability

### Agent Metrics

**JMX Metrics (via Prometheus Exporter):**
- Commit logs processed
- Mutations sent
- Mutations skipped (unsupported types)
- Pulsar producer metrics
- Processing latency

**Configuration:**
```yaml
# jmx_prometheus_exporter.yaml
rules:
  - pattern: ".*"
```

### Connector Metrics

**JMX Metrics:**
- Events consumed
- Cassandra queries executed
- Query latency (mobile average)
- Cache hit/miss ratio
- Executor pool size
- Backoff events

**Pulsar Metrics:**
- Source connector throughput
- Message processing latency
- Subscription lag
- Schema updates

### Logging

**Agent Logging:**
- Logback configuration
- Log levels: DEBUG, INFO, WARN, ERROR
- File rotation and retention
- Commit log processing events

**Connector Logging:**
- Pulsar function logging
- Query execution details
- Cache statistics
- Error handling

## Security

### SSL/TLS Support

**Agent:**
- Pulsar client SSL/TLS
- Truststore and keystore configuration
- Certificate chain validation
- Hostname verification
- Cipher suite selection

**Connector:**
- Cassandra driver SSL/TLS
- Pulsar client SSL/TLS
- Cloud secure bundle support (Astra)
- Base64-encoded certificates

### Authentication

**Agent:**
- Pulsar authentication plugins
- Token-based authentication
- OAuth 2.0 support
- Custom authentication

**Connector:**
- Cassandra authentication (username/password)
- LDAP integration
- Kerberos support (via keytab)
- Cloud authentication (Astra)

## Deployment Patterns

### 1. Single Datacenter

```
Cassandra DC1 (3 nodes)
    ├─ Node 1 (CDC Agent)
    ├─ Node 2 (CDC Agent)
    └─ Node 3 (CDC Agent)
         │
         ▼
    Pulsar Cluster
         │
         ├─ Events Topics (partitioned)
         └─ Data Topics (partitioned)
              │
              └─ Cassandra Source Connector (3 instances)
```

### 2. Multi-Datacenter

**Recommended:** Enable CDC in only ONE datacenter.

```
Cassandra DC1 (CDC enabled)     Cassandra DC2 (CDC disabled)
    ├─ Node 1 (CDC Agent)           ├─ Node 1
    ├─ Node 2 (CDC Agent)           ├─ Node 2
    └─ Node 3 (CDC Agent)           └─ Node 3
         │                               │
         │                               │
         └───────────────┬───────────────┘
                         │
                    Replication
                         │
                         ▼
                   Pulsar Cluster
```

**Keyspace Replication:**
```cql
CREATE KEYSPACE ks1 WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'dc1': 3,  -- CDC enabled
  'dc2': 3   -- CDC disabled, but data replicated
};
```

### 3. Kubernetes Deployment

**Components:**
- Cassandra StatefulSet with CDC agent sidecar
- Pulsar cluster (Helm chart)
- Cassandra Source Connector (Pulsar Function)

**Configuration:**
- ConfigMaps for agent and connector settings
- Secrets for credentials and certificates
- PersistentVolumes for CDC working directory

## Limitations and Constraints

### Functional Limitations

1. **No Truncate Support**
   - TRUNCATE operations are not captured
   - Workaround: Use DELETE statements

2. **No Historical Data**
   - Only captures changes after agent start
   - Use backfill CLI for historical data

3. **No Logged Batches**
   - Batch operations not replayed
   - Individual mutations are captured

4. **No TTL Management**
   - TTL expiration not captured
   - Downstream systems must handle TTL

5. **No Range Deletes**
   - Range delete operations not supported
   - Individual row deletes are captured

6. **Column Name Restrictions**
   - Cannot match Pulsar primitive type names
   - (BOOLEAN, INT8, INT16, INT32, INT64, FLOAT, DOUBLE, BYTES, STRING, TIMESTAMP)

### Performance Limitations

1. **Commit Log Sync Period**
   - C3: Minimum 2 seconds (configurable)
   - C4/DSE4: Near real-time (10 seconds default)

2. **De-duplication Overhead**
   - MD5 digest calculation per mutation
   - Cache memory usage
   - Slight latency increase

3. **Query Amplification**
   - One Cassandra query per unique mutation
   - Cache reduces but doesn't eliminate queries
   - Impacts Cassandra cluster load

## Build and Development

### Build System

**Gradle 7.x:**
- Multi-module project
- Shadow plugin for uber JARs
- NAR plugin for Pulsar connectors
- Docker plugin for container images
- License management and reporting

### Build Commands

```bash
# Build all modules
./gradlew build

# Build specific module
./gradlew agent-c4:build

# Build distributions
./gradlew agent-distribution:assemble
./gradlew connector-distribution:assemble
./gradlew backfill-cli:assemble

# Run tests
./gradlew test

# Run integration tests
./gradlew agent-c4:test
./gradlew connector:test
./gradlew backfill-cli:e2eTest -PcassandraFamily=c4

# Generate documentation
./gradlew docs:build
```

### Dependencies

**Key Libraries:**
- Apache Pulsar Client: 3.0.3
- Apache Avro: 1.11.4
- DataStax Java Driver: 4.19.2
- Caffeine Cache: 2.8.8
- Lombok: 1.18.20
- SLF4J/Logback: 1.7.30 / 1.5.27
- JUnit Jupiter: 5.7.2
- Testcontainers: 1.19.1

**Version-Specific:**
- Cassandra 3: 3.11.19
- Cassandra 4: 4.0.4
- DSE 4: 6.8.61

### Testing Strategy

**Unit Tests:**
- Configuration validation
- Converter logic
- Cache behavior
- Utility functions

**Integration Tests:**
- Single-node Cassandra + Pulsar
- Multi-node replication
- Schema evolution
- Error handling

**End-to-End Tests:**
- Full CDC pipeline
- Backfill scenarios
- Multiple Cassandra versions
- Different output formats

## Future Enhancements

### Potential Improvements

1. **Performance:**
   - Parallel commit log processing
   - Batch Cassandra queries
   - Improved caching strategies

2. **Features:**
   - TTL support
   - Range delete support
   - Truncate handling
   - Materialized view support

3. **Operations:**
   - Kubernetes operator
   - Automated backfill
   - Enhanced monitoring
   - Configuration validation

4. **Compatibility:**
   - Additional Cassandra versions
   - Alternative streaming platforms
   - Cloud-native deployments

## Conclusion

CDC for Apache Cassandra provides a robust, production-ready solution for change data capture from Cassandra to Apache Pulsar. The architecture balances performance, reliability, and ease of deployment while supporting multiple Cassandra versions and flexible output formats. The modular design allows for version-specific optimizations while maintaining a consistent API and configuration model.