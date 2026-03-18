# Phase 4: Kafka Implementation - Implementation Plan

**Version:** 1.0  
**Date:** 2026-03-18  
**Status:** Planning  
**Estimated Duration:** 4 weeks (20 working days)

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Prerequisites and Dependencies](#2-prerequisites-and-dependencies)
3. [Implementation Objectives](#3-implementation-objectives)
4. [Kafka vs Pulsar Feature Analysis](#4-kafka-vs-pulsar-feature-analysis)
5. [Detailed Implementation Plan](#5-detailed-implementation-plan)
6. [Module Structure](#6-module-structure)
7. [Configuration Mapping Strategy](#7-configuration-mapping-strategy)
8. [Testing Strategy](#8-testing-strategy)
9. [Build and CI Integration](#9-build-and-ci-integration)
10. [Risk Mitigation](#10-risk-mitigation)
11. [Success Criteria](#11-success-criteria)
12. [Appendices](#appendices)

---

## 1. Executive Summary

### 1.1 Purpose

Phase 4 implements Kafka-specific adapters for the messaging abstraction layer, enabling CDC for Apache Cassandra to support Apache Kafka as an alternative messaging backend alongside Apache Pulsar. This phase focuses on creating a production-ready Kafka implementation while maintaining 100% backward compatibility with existing Pulsar deployments.

### 1.2 Key Deliverables

1. **messaging-kafka Module** - New Gradle module with Kafka implementations
2. **Kafka Adapter Classes** (10 core classes):
   - KafkaMessagingClient
   - KafkaMessageProducer
   - KafkaMessageConsumer
   - KafkaMessage
   - KafkaMessageId
   - KafkaSchemaProvider (with Schema Registry integration)
   - KafkaClientProvider (SPI)
   - KafkaConfigMapper
   - KafkaOffsetTracker (for acknowledgment semantics)
   - KafkaTransactionManager (optional, for exactly-once semantics)
3. **Schema Registry Integration** - Confluent Schema Registry support for AVRO schemas
4. **Agent Kafka Support** - Configuration and runtime support for Kafka in CDC agents
5. **Connector Kafka Support** - Kafka consumer implementation for CassandraSource
6. **Integration Tests** - Full test coverage for Kafka implementations
7. **Performance Benchmarks** - Validation that Kafka performance meets targets
8. **Migration Documentation** - Guide for switching from Pulsar to Kafka

### 1.3 Non-Goals (Out of Scope)

- Changes to existing Pulsar implementation
- New CDC features or capabilities
- Kafka Streams integration
- Kafka Connect framework integration (separate from Pulsar connector)
- Multi-datacenter Kafka replication configuration
- Breaking changes to existing APIs or configurations

### 1.4 Design Constraints

- **No Functionality Breakage**: All existing Pulsar functionality must continue working
- **DRY Principles**: Reuse abstractions from messaging-api and base implementations
- **Build Stability**: All CI jobs must pass before and after implementation
- **Performance Parity**: Kafka implementation must achieve ≥95% of Pulsar performance
- **Configuration Compatibility**: Kafka configuration should mirror Pulsar where possible

---

## 2. Prerequisites and Dependencies

### 2.1 Completed Phases

**Phase 1: Design and Interface Definition** ✅
- All interfaces defined in `messaging-api` module
- Configuration model established
- ADR documented

**Phase 2: Core Abstraction Layer** ✅
- Base implementation classes
- Configuration builders
- Statistics implementations
- Factory patterns

**Phase 3: Pulsar Implementation** ✅
- Pulsar adapters implemented
- Agent and Connector migrated
- Integration tests passing
- Performance validated

### 2.2 Required Dependencies

**New Kafka Dependencies:**
```gradle
dependencies {
    api project(':messaging-api')
    
    // Kafka Client
    implementation 'org.apache.kafka:kafka-clients:4.2.0'
    
    // Schema Registry Client (Confluent)
    implementation 'io.confluent:kafka-avro-serializer:8.2.0'
    implementation 'io.confluent:kafka-schema-registry-client:8.2.0'
    
    // AVRO (already in project)
    implementation 'org.apache.avro:avro:1.12.1'
    
    // Logging (already in project)
    implementation 'org.slf4j:slf4j-api:1.7.36'
    
    // Testing
    testImplementation 'org.apache.kafka:kafka_2.13:4.2.0'
    testImplementation 'org.testcontainers:kafka:1.21.4'
}
```

**Repository Configuration:**
```gradle
repositories {
    mavenCentral()
    maven {
        url "https://packages.confluent.io/maven/"
    }
}
```

### 2.3 Build Environment

- Gradle 7.x
- Java 11+
- Kafka 4.2.0+ (for testing; refer to https://kafka.apache.org/42/)
- Confluent Platform 8.2+ (for Schema Registry; refer https://docs.confluent.io/platform/current/schema-registry/develop/api.html)
- All existing CI jobs must pass
- New Kafka-specific CI jobs

### 2.4 External Services

**Development/Testing:**
- Kafka broker (via Testcontainers)
- Zookeeper (if using older Kafka versions)
- Confluent Schema Registry (via Testcontainers)

**Production:**
- Kafka cluster (3+ brokers recommended)
- Schema Registry cluster (3+ nodes recommended)
- Monitoring infrastructure (Prometheus, Grafana)

---

## 3. Implementation Objectives

### 3.1 Primary Goals

1. **Kafka Adapter Implementation**: Complete, production-ready Kafka adapters
2. **Schema Registry Integration**: Full AVRO schema management via Confluent Schema Registry
3. **Semantic Compatibility**: Bridge Kafka and Pulsar semantic differences
4. **Agent Support**: Enable CDC agents to publish to Kafka topics
5. **Connector Support**: Enable connector to consume from Kafka topics
6. **Testing Coverage**: ≥90% code coverage for Kafka module
7. **Performance Validation**: Meet or exceed performance targets
8. **Documentation**: Complete migration and configuration guides

### 3.2 Design Principles

1. **Reuse Abstractions**: Leverage messaging-api interfaces and base classes
2. **Kafka Best Practices**: Follow Kafka producer/consumer best practices
3. **Idempotency**: Support idempotent producers for exactly-once semantics
4. **Offset Management**: Proper offset tracking for acknowledgment semantics
5. **Error Handling**: Robust error handling and retry logic
6. **Resource Management**: Proper cleanup and connection pooling
7. **Monitoring**: Comprehensive metrics and observability

### 3.3 Quality Standards

- **Code Coverage**: ≥90% for messaging-kafka module
- **Build Success**: 100% CI job pass rate
- **Performance**: ≥95% of Pulsar throughput and latency
- **Documentation**: Complete API docs and migration guides
- **Backward Compatibility**: Zero breaking changes to existing code

---

## 4. Kafka vs Pulsar Feature Analysis

### 4.1 Feature Parity Matrix

| Feature | Pulsar | Kafka | Implementation Strategy |
|---------|--------|-------|------------------------|
| **Message Ordering** | ✅ Per-key | ✅ Per-partition | Map key-based routing to partition keys |
| **Acknowledgment** | ✅ Individual | ⚠️ Offset-based | Implement offset tracking in KafkaOffsetTracker |
| **Negative Ack** | ✅ Built-in | ⚠️ Manual (seek) | Implement via offset reset and DLQ pattern |
| **Schema Evolution** | ✅ Built-in Registry | ✅ Confluent SR | Integrate Confluent Schema Registry |
| **Transactions** | ⚠️ Limited | ✅ Full support | Implement KafkaTransactionManager |
| **Batching** | ✅ Configurable | ✅ Configurable | Map batch configs directly |
| **Compression** | ✅ Multiple types | ✅ Multiple types | Map compression types |
| **Key-Value Schema** | ✅ Native | ✅ Via SR | Use separate key/value serializers |
| **Subscription Types** | ✅ Multiple | ⚠️ Consumer Groups | Map to consumer group semantics |
| **Message Properties** | ✅ Headers | ✅ Headers | Map properties to Kafka headers |
| **TTL** | ✅ Message-level | ⚠️ Topic-level | Document limitation |
| **Dead Letter Queue** | ✅ Built-in | ⚠️ Manual | Implement DLQ pattern |

### 4.2 Semantic Differences and Solutions

#### 4.2.1 Acknowledgment Model

**Pulsar**: Individual message acknowledgment
**Kafka**: Offset-based acknowledgment

**Solution**: Implement `KafkaOffsetTracker` to track offsets per message and commit appropriately, ensuring contiguous offset commits while supporting individual message acknowledgment semantics.

#### 4.2.2 Negative Acknowledgment

**Pulsar**: Built-in negative acknowledgment
**Kafka**: Manual seek to retry

**Solution**: Implement retry logic with configurable backoff and DLQ pattern for failed messages after max retries.

#### 4.2.3 Subscription Types

**Pulsar Subscription Types**: Exclusive, Failover, Shared, Key_Shared
**Kafka Consumer Groups**: Single consumer group with partition assignment

**Solution**: Map Pulsar subscription types to Kafka consumer group configurations:
- Exclusive → Single consumer in group
- Failover → Consumer group with static membership
- Shared → Consumer group with round-robin assignment
- Key_Shared → Consumer group with sticky assignor

#### 4.2.4 Schema Registry

**Pulsar**: Built-in schema registry
**Kafka**: Confluent Schema Registry

**Solution**: Integrate Confluent Schema Registry with automatic schema registration and compatibility checking.

---

## 5. Detailed Implementation Plan

### 5.1 Week 1: Core Kafka Adapters (Days 1-5)

#### Day 1: Module Setup and KafkaMessagingClient

**Objectives:**
- Create messaging-kafka module structure
- Implement KafkaMessagingClient
- Set up Kafka dependencies

**Tasks:**

1. **Create Module Structure** (2 hours)
2. **Implement KafkaMessagingClient** (4 hours)
3. **Implement KafkaClientProvider (SPI)** (2 hours)

**Deliverables:**
- messaging-kafka module created
- KafkaMessagingClient implemented
- KafkaClientProvider SPI configured
- Unit tests passing

#### Day 2: KafkaMessageProducer and Idempotency

**Objectives:**
- Implement KafkaMessageProducer
- Support idempotent producers
- Handle batching and compression

**Tasks:**

1. **Implement KafkaMessageProducer** (5 hours)
2. **Implement KafkaTransactionManager** (3 hours)

**Deliverables:**
- KafkaMessageProducer implemented
- Idempotent producer support
- Transaction support (optional)
- Unit tests passing

#### Day 3: KafkaMessageConsumer and Offset Tracking

**Objectives:**
- Implement KafkaMessageConsumer
- Implement KafkaOffsetTracker
- Handle acknowledgment semantics

**Tasks:**

1. **Implement KafkaOffsetTracker** (3 hours)
2. **Implement KafkaMessageConsumer** (5 hours)

**Deliverables:**
- KafkaMessageConsumer implemented
- KafkaOffsetTracker implemented
- Acknowledgment semantics working
- Unit tests passing

#### Day 4: Message and MessageId Wrappers

**Objectives:**
- Implement KafkaMessage
- Implement KafkaMessageId
- Handle Kafka headers as properties

**Tasks:**

1. **Implement KafkaMessageId** (2 hours)
2. **Implement KafkaMessage** (4 hours)

**Deliverables:**
- KafkaMessage implemented
- KafkaMessageId implemented
- Header/property conversion working
- Unit tests passing

#### Day 5: Configuration Mapper and Schema Provider

**Objectives:**
- Implement KafkaConfigMapper
- Implement KafkaSchemaProvider
- Integrate Confluent Schema Registry

**Tasks:**

1. **Implement KafkaConfigMapper** (4 hours)
2. **Implement KafkaSchemaProvider** (4 hours)

**Deliverables:**
- KafkaConfigMapper implemented
- KafkaSchemaProvider implemented
- Schema Registry integration working
- Unit tests passing

---

### 5.2 Week 2: Agent Kafka Support (Days 6-10)

#### Day 6-7: Agent Configuration and Kafka Support

**Objectives:**
- Add Kafka configuration to AgentConfig
- Create Kafka-aware MutationSender
- Maintain backward compatibility

**Tasks:**

1. **Update AgentConfig for Kafka** (4 hours)
2. **Create AbstractMessagingMutationSender** (6 hours)
3. **Refactor AbstractPulsarMutationSender** (4 hours)
4. **Create KafkaMutationSender** (4 hours)

**Deliverables:**
- AgentConfig supports Kafka configuration
- AbstractMessagingMutationSender base class created
- Pulsar implementation refactored to use base class
- Kafka implementation created
- Backward compatibility maintained

#### Day 8-9: Update Version-Specific Agents

**Objectives:**
- Update agent-c3, agent-c4, agent-dse4 for Kafka
- Implement Kafka-specific mutation senders
- Test with both Pulsar and Kafka

**Tasks:**

1. **Update agent-c4 for Kafka** (6 hours)
2. **Update agent-c3 for Kafka** (4 hours)
3. **Update agent-dse4 for Kafka** (4 hours)
4. **Add Kafka dependencies to agent modules** (2 hours)

**Deliverables:**
- All agent modules support Kafka
- Version-specific Kafka mutation senders implemented
- Build successful for all agent modules
- Unit tests passing

#### Day 10: Agent Integration Testing

**Objectives:**
- Test agents with Kafka
- Verify mutation publishing
- Performance validation

**Tasks:**

1. **Create Kafka integration tests** (4 hours)
2. **Test C4 agent with Kafka** (2 hours)
3. **Test C3 agent with Kafka** (2 hours)

**Deliverables:**
- Integration tests passing
- Agents successfully publish to Kafka
- Performance metrics collected

---

### 5.3 Week 3: Connector Kafka Support (Days 11-15)

#### Day 11-12: Connector Configuration and Kafka Support

**Objectives:**
- Add Kafka configuration to CassandraSourceConnectorConfig
- Create Kafka-aware consumer in CassandraSource
- Maintain backward compatibility

**Tasks:**

1. **Update CassandraSourceConnectorConfig** (4 hours)
2. **Refactor CassandraSource for abstraction** (8 hours)
3. **Implement Kafka consumer logic** (6 hours)

**Deliverables:**
- Connector config supports Kafka
- CassandraSource uses messaging abstraction
- Kafka consumer implementation working
- Backward compatibility maintained

#### Day 13: Connector Integration Testing

**Objectives:**
- Test connector with Kafka
- Verify end-to-end flow
- Test schema evolution

**Tasks:**

1. **Create Kafka connector tests** (4 hours)
2. **Test end-to-end CDC flow** (3 hours)
3. **Test schema evolution** (1 hour)

**Deliverables:**
- Integration tests passing
- End-to-end flow working
- Schema evolution verified

#### Day 14: Performance Benchmarking

**Objectives:**
- Benchmark Kafka implementation
- Compare with Pulsar performance
- Identify optimization opportunities

**Tasks:**

1. **Create performance test suite** (3 hours)
2. **Run throughput benchmarks** (2 hours)
3. **Run latency benchmarks** (2 hours)
4. **Analyze results and optimize** (1 hour)

**Deliverables:**
- Performance benchmarks completed
- Results documented
- Optimizations identified

#### Day 15: Documentation and Migration Guide

**Objectives:**
- Document Kafka configuration
- Create migration guide
- Update README files

**Tasks:**

1. **Document Kafka configuration** (3 hours)
2. **Create migration guide** (3 hours)
3. **Update README files** (2 hours)

**Deliverables:**
- Configuration documentation complete
- Migration guide published
- README files updated

---

### 5.4 Week 4: Testing, Optimization, and Finalization (Days 16-20)

#### Day 16-17: Comprehensive Testing

**Objectives:**
- Run full test suite
- Test failure scenarios
- Test recovery mechanisms

**Tasks:**

1. **Run all unit tests** (2 hours)
2. **Run all integration tests** (3 hours)
3. **Test failure scenarios** (4 hours)
4. **Test recovery mechanisms** (3 hours)

**Deliverables:**
- All tests passing
- Failure scenarios handled
- Recovery mechanisms verified

#### Day 18: Performance Optimization

**Objectives:**
- Optimize based on benchmarks
- Tune Kafka configurations
- Validate improvements

**Tasks:**

1. **Optimize producer settings** (3 hours)
2. **Optimize consumer settings** (3 hours)
3. **Re-run benchmarks** (2 hours)

**Deliverables:**
- Performance optimizations applied
- Benchmarks show improvements
- Configuration tuning documented

#### Day 19: CI/CD Integration

**Objectives:**
- Update CI pipelines
- Add Kafka-specific jobs
- Verify all jobs pass

**Tasks:**

1. **Update GitHub Actions workflows** (3 hours)
2. **Add Kafka integration tests to CI** (2 hours)
3. **Verify all CI jobs pass** (3 hours)

**Deliverables:**
- CI pipelines updated
- Kafka tests in CI
- All jobs passing

#### Day 20: Final Review and Documentation

**Objectives:**
- Code review
- Documentation review
- Release preparation

**Tasks:**

1. **Conduct code review** (3 hours)
2. **Review documentation** (2 hours)
3. **Prepare release notes** (2 hours)
4. **Final verification** (1 hour)

**Deliverables:**
- Code review complete
- Documentation finalized
- Release notes prepared
- Phase 4 complete

---

## 6. Module Structure

### 6.1 New Module: messaging-kafka

```
messaging-kafka/
├── build.gradle
├── src/
│   ├── main/
│   │   ├── java/com/datastax/oss/cdc/messaging/kafka/
│   │   │   ├── KafkaMessagingClient.java
│   │   │   ├── KafkaMessageProducer.java
│   │   │   ├── KafkaMessageConsumer.java
│   │   │   ├── KafkaMessage.java
│   │   │   ├── KafkaMessageId.java
│   │   │   ├── KafkaSchemaProvider.java
│   │   │   ├── KafkaClientProvider.java
│   │   │   ├── KafkaConfigMapper.java
│   │   │   ├── KafkaOffsetTracker.java
│   │   │   └── KafkaTransactionManager.java
│   │   └── resources/
│   │       └── META-INF/services/
│   │           └── com.datastax.oss.cdc.messaging.MessagingClientProvider
│   └── test/
│       ├── java/com/datastax/oss/cdc/messaging/kafka/
│       │   ├── KafkaMessagingClientTest.java
│       │   ├── KafkaMessageProducerTest.java
│       │   ├── KafkaMessageConsumerTest.java
│       │   ├── KafkaSchemaProviderTest.java
│       │   ├── KafkaConfigMapperTest.java
│       │   ├── KafkaOffsetTrackerTest.java
│       │   └── KafkaIntegrationTest.java
│       └── resources/
│           └── logback-test.xml
```

### 6.2 Updated Modules

**agent module:**
- Add `AbstractMessagingMutationSender.java`
- Add `KafkaMutationSender.java`
- Update `AgentConfig.java` for Kafka configuration
- Update `build.gradle` to include messaging-kafka dependency

**agent-c3, agent-c4, agent-dse4 modules:**
- Implement version-specific Kafka mutation senders
- Update build.gradle dependencies

**connector module:**
- Update `CassandraSource.java` to use messaging abstraction
- Update `CassandraSourceConnectorConfig.java` for Kafka configuration
- Update `build.gradle` to include messaging-kafka dependency

### 6.3 Gradle Configuration

**settings.gradle:**
```gradle
include 'messaging-kafka'
```

**messaging-kafka/build.gradle:**
```gradle
plugins {
    id 'java-library'
}

dependencies {
    api project(':messaging-api')
    
    implementation 'org.apache.kafka:kafka-clients:3.6.1'
    implementation 'io.confluent:kafka-avro-serializer:7.5.3'
    implementation 'io.confluent:kafka-schema-registry-client:7.5.3'
    implementation 'org.apache.avro:avro:1.11.4'
    implementation 'org.slf4j:slf4j-api:1.7.30'
    
    testImplementation 'org.junit.jupiter:junit-jupiter:5.7.2'
    testImplementation 'org.mockito:mockito-core:3.11.2'
    testImplementation 'org.apache.kafka:kafka_2.13:3.6.1'
    testImplementation 'org.testcontainers:kafka:1.19.1'
    testImplementation 'org.testcontainers:junit-jupiter:1.19.1'
}

repositories {
    mavenCentral()
    maven {
        url "https://packages.confluent.io/maven/"
    }
}

test {
    useJUnitPlatform()
}
```

---

## 7. Configuration Mapping Strategy

### 7.1 Agent Configuration Mapping

| Pulsar Parameter | Kafka Equivalent | Mapping Strategy |
|------------------|------------------|------------------|
| `pulsarServiceUrl` | `kafkaBootstrapServers` | Direct mapping to bootstrap.servers |
| `pulsarBatchDelayInMs` | `kafkaLingerMs` | Map to linger.ms |
| `pulsarKeyBasedBatcher` | N/A | Use partition key for routing |
| `pulsarMaxPendingMessages` | `kafkaMaxInFlightRequests` | Map to max.in.flight.requests.per.connection |
| `pulsarMemoryLimitBytes` | `kafkaBufferMemory` | Map to buffer.memory |
| `pulsarAuthPluginClassName` | `kafkaSaslMechanism` | Map to SASL configuration |
| `pulsarAuthParams` | `kafkaSaslJaasConfig` | Map to JAAS configuration |

### 7.2 Connector Configuration Mapping

| Pulsar Parameter | Kafka Equivalent | Mapping Strategy |
|------------------|------------------|------------------|
| `events.topic` | `kafka.topic` | Direct mapping |
| `events.subscription.name` | `kafka.group.id` | Map to consumer group ID |
| `events.subscription.type` | `kafka.partition.assignment.strategy` | Map subscription types to assignment strategies |
| `batch.size` | `kafka.max.poll.records` | Map to max.poll.records |

### 7.3 Configuration Example

**Agent Configuration (Kafka):**
```properties
messagingProvider=KAFKA
kafkaBootstrapServers=localhost:9092
kafkaSchemaRegistryUrl=http://localhost:8081
kafkaProducerAcks=all
kafkaCompressionType=lz4
kafkaBatchSize=16384
kafkaLingerMs=10
kafkaEnableIdempotence=true
topicPrefix=events-
```

**Connector Configuration (Kafka):**
```yaml
configs:
  messagingProvider: "KAFKA"
  kafkaBootstrapServers: "localhost:9092"
  kafkaSchemaRegistryUrl: "http://localhost:8081"
  kafkaTopic: "events-ks1.table1"
  kafkaGroupId: "cassandra-source-connector"
  kafkaAutoOffsetReset: "earliest"
  kafkaMaxPollRecords: 500
```

---

## 8. Testing Strategy

### 8.1 Unit Tests

**Coverage Target**: ≥90% for messaging-kafka module

**Test Classes:**
1. `KafkaMessagingClientTest` - Client lifecycle and configuration
2. `KafkaMessageProducerTest` - Producer operations and error handling
3. `KafkaMessageConsumerTest` - Consumer operations and acknowledgment
4. `KafkaSchemaProviderTest` - Schema registration and retrieval
5. `KafkaConfigMapperTest` - Configuration mapping logic
6. `KafkaOffsetTrackerTest` - Offset tracking and commit logic
7. `KafkaTransactionManagerTest` - Transaction management

**Test Scenarios:**
- Configuration validation
- Producer send operations (sync/async)
- Consumer receive and acknowledgment
- Offset tracking and commit
- Schema registration and evolution
- Error handling and recovery
- Resource cleanup

### 8.2 Integration Tests

**Test Infrastructure:**
- Testcontainers for Kafka broker
- Testcontainers for Schema Registry
- Testcontainers for Cassandra

**Test Classes:**
1. `KafkaIntegrationTest` - Basic producer/consumer flow
2. `KafkaAgentIntegrationTest` - Agent publishing to Kafka
3. `KafkaConnectorIntegrationTest` - Connector consuming from Kafka
4. `KafkaEndToEndTest` - Complete CDC flow with Kafka

**Test Scenarios:**
- Message production and consumption
- Schema evolution
- Offset management
- Failure recovery
- Performance under load

### 8.3 End-to-End Tests

**Test Scenarios:**
1. **Basic CDC Flow**:
   - Start Cassandra with CDC agent (Kafka mode)
   - Perform INSERT/UPDATE/DELETE operations
   - Verify mutations published to Kafka
   - Start connector
   - Verify data topics populated

2. **Schema Evolution**:
   - Add column to Cassandra table
   - Verify schema updated in Schema Registry
   - Verify connector handles new schema

3. **Failure Recovery**:
   - Simulate Kafka broker failure
   - Verify agent retry logic
   - Verify no data loss

4. **Performance**:
   - High-throughput workload
   - Measure latency and throughput
   - Compare with Pulsar baseline

### 8.4 Performance Tests

**Metrics to Measure:**
- Throughput (messages/second)
- Latency (P50, P95, P99)
- Resource usage (CPU, memory)
- Network bandwidth

**Test Scenarios:**
1. **Producer Performance**:
   - Measure send throughput
   - Measure send latency
   - Test with different batch sizes

2. **Consumer Performance**:
   - Measure receive throughput
   - Measure processing latency
   - Test with different poll sizes

3. **End-to-End Performance**:
   - Measure total CDC latency
   - Test with realistic workload
   - Compare with Pulsar performance

**Performance Targets:**
- Throughput: ≥95% of Pulsar performance
- Latency P99: ≤5% increase over Pulsar
- Memory: ≤10% increase over Pulsar
- CPU: ≤5% increase over Pulsar

---

## 9. Build and CI Integration

### 9.1 Gradle Build Updates

**Root build.gradle:**
```gradle
// No changes needed - messaging-kafka follows existing patterns
```

**CI Build Commands:**
```bash
# Build all modules including Kafka
./gradlew build

# Build only Kafka module
./gradlew messaging-kafka:build

# Run Kafka tests
./gradlew messaging-kafka:test

# Run Kafka integration tests
./gradlew messaging-kafka:integrationTest

# Build agent with Kafka support
./gradlew agent-c4:build

# Build connector with Kafka support
./gradlew connector:build
```

### 9.2 CI Pipeline Configuration

**GitHub Actions Workflow (.github/workflows/kafka-ci.yaml):**
```yaml
name: Kafka Integration Tests

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main, develop ]

jobs:
  kafka-tests:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Set up JDK 11
      uses: actions/setup-java@v3
      with:
        java-version: '11'
        distribution: 'temurin'
    
    - name: Cache Gradle packages
      uses: actions/cache@v3
      with:
        path: ~/.gradle/caches
        key: ${{ runner.os }}-gradle-${{ hashFiles('**/*.gradle') }}
        restore-keys: ${{ runner.os }}-gradle
    
    - name: Build messaging-kafka
      run: ./gradlew messaging-kafka:build
    
    - name: Run Kafka unit tests
      run: ./gradlew messaging-kafka:test
    
    - name: Run Kafka integration tests
      run: ./gradlew messaging-kafka:integrationTest
    
    - name: Build agent with Kafka
      run: ./gradlew agent-c4:build
    
    - name: Build connector with Kafka
      run: ./gradlew connector:build
    
    - name: Upload test results
      if: always()
      uses: actions/upload-artifact@v3
      with:
        name: kafka-test-results
        path: messaging-kafka/build/reports/tests/
```

### 9.3 Build Verification Steps

**Pre-Implementation Verification:**
```bash
# Verify all existing tests pass
./gradlew clean build test

# Verify CI jobs pass
# Check GitHub Actions status
```

**Post-Implementation Verification:**
```bash
# Verify messaging-kafka builds
./gradlew messaging-kafka:build

# Verify all tests pass
./gradlew test

# Verify integration tests pass
./gradlew integrationTest

# Verify agent builds with Kafka
./gradlew agent-c4:build

# Verify connector builds with Kafka
./gradlew connector:build

# Verify distributions build
./gradlew agent-distribution:assemble
./gradlew connector-distribution:assemble

# Verify CI jobs pass
# Check GitHub Actions status
```

---

## 10. Risk Mitigation

### 10.1 Identified Risks

1. **Schema Registry Dependency**
   - Risk: Additional external dependency
   - Impact: High
   - Probability: Medium

2. **Offset Management Complexity**
   - Risk: Incorrect offset tracking leading to data loss/duplication
   - Impact: High
   - Probability: Medium

3. **Performance Degradation**
   - Risk: Kafka implementation slower than Pulsar
   - Impact: Medium
   - Probability: Low

4. **Configuration Complexity**
   - Risk: Users confused by dual configuration
   - Impact: Medium
   - Probability: Medium

5. **Build Time Increase**
   - Risk: Additional module increases build time
   - Impact: Low
   - Probability: High

### 10.2 Mitigation Strategies

**Schema Registry Dependency:**
- Mitigation: Make Schema Registry optional for non-AVRO use cases
- Fallback: Support embedded schemas for simple deployments
- Documentation: Clear setup guide for Schema Registry

**Offset Management:**
- Mitigation: Comprehensive unit tests for offset tracking
- Validation: Integration tests with failure scenarios
- Monitoring: Expose offset lag metrics

**Performance:**
- Mitigation: Early performance testing and optimization
- Benchmarking: Compare with Pulsar baseline
- Tuning: Document optimal Kafka configurations

**Configuration:**
- Mitigation: Clear migration guide
- Examples: Provide configuration examples for both platforms
- Validation: Configuration validation at startup

**Build Time:**
- Mitigation: Parallel builds in CI
- Optimization: Gradle build cache
- Selective: Allow building without Kafka module

### 10.3 Rollback Procedures

**If Critical Issues Found:**

1. **Revert Kafka Changes**:
   ```bash
   git revert <kafka-implementation-commits>
   ```

2. **Disable Kafka Module**:
   - Remove from settings.gradle
   - Remove dependencies from agent/connector

3. **Restore Pulsar-Only Mode**:
   - Ensure all Pulsar functionality intact
   - Run full test suite
   - Verify CI passes

4. **Communication**:
   - Notify stakeholders
   - Document issues found
   - Plan remediation

---

## 11. Success Criteria

### 11.1 Functional Success Criteria

- [ ] messaging-kafka module builds successfully
- [ ] All Kafka adapter classes implemented
- [ ] Schema Registry integration working
- [ ] Agent can publish to Kafka topics
- [ ] Connector can consume from Kafka topics
- [ ] End-to-end CDC flow working with Kafka
- [ ] Schema evolution supported
- [ ] Offset management working correctly
- [ ] Error handling and recovery working
- [ ] All existing Pulsar functionality intact

### 11.2 Quality Success Criteria

- [ ] Code coverage ≥90% for messaging-kafka module
- [ ] All unit tests passing
- [ ] All integration tests passing
- [ ] All end-to-end tests passing
- [ ] No new compiler warnings
- [ ] No new static analysis issues
- [ ] Code review approved
- [ ] Documentation complete

### 11.3 Performance Success Criteria

- [ ] Throughput ≥95% of Pulsar performance
- [ ] Latency P99 ≤5% increase over Pulsar
- [ ] Memory usage ≤10% increase over Pulsar
- [ ] CPU usage ≤5% increase over Pulsar
- [ ] No resource leaks detected
- [ ] Performance benchmarks documented

### 11.4 Build and CI Success Criteria

- [ ] All CI jobs passing
- [ ] Build time increase ≤10%
- [ ] No dependency conflicts
- [ ] Distributions build successfully
- [ ] Docker images build successfully
- [ ] Release artifacts generated

### 11.5 Documentation Success Criteria

- [ ] Kafka configuration documented
- [ ] Migration guide published
- [ ] API documentation complete
- [ ] README files updated
- [ ] Examples provided
- [ ] Troubleshooting guide created

---

## Appendices

### Appendix A: Configuration Reference

**Agent Kafka Configuration Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `messagingProvider` | String | PULSAR | Messaging platform (PULSAR or KAFKA) |
| `kafkaBootstrapServers` | String | localhost:9092 | Kafka broker addresses |
| `kafkaSchemaRegistryUrl` | String | http://localhost:8081 | Schema Registry URL |
| `kafkaProducerAcks` | String | all | Producer acknowledgment mode |
| `kafkaCompressionType` | String | lz4 | Compression algorithm |
| `kafkaBatchSize` | Integer | 16384 | Batch size in bytes |
| `kafkaLingerMs` | Long | 10 | Batching delay in milliseconds |
| `kafkaEnableIdempotence` | Boolean | true | Enable idempotent producer |
| `kafkaTransactionalId` | String | null | Transactional ID (optional) |
| `kafkaSaslMechanism` | String | null | SASL mechanism (PLAIN, SCRAM, etc.) |
| `kafkaSaslJaasConfig` | String | null | JAAS configuration |
| `kafkaSslEnabled` | Boolean | false | Enable SSL/TLS |
| `kafkaSslTruststoreLocation` | String | null | Truststore file path |
| `kafkaSslTruststorePassword` | String | null | Truststore password |
| `kafkaSslKeystoreLocation` | String | null | Keystore file path |
| `kafkaSslKeystorePassword` | String | null | Keystore password |

**Connector Kafka Configuration Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `messagingProvider` | String | PULSAR | Messaging platform (PULSAR or KAFKA) |
| `kafkaBootstrapServers` | String | localhost:9092 | Kafka broker addresses |
| `kafkaSchemaRegistryUrl` | String | http://localhost:8081 | Schema Registry URL |
| `kafkaTopic` | String | Required | Kafka topic to consume from |
| `kafkaGroupId` | String | Required | Consumer group ID |
| `kafkaAutoOffsetReset` | String | earliest | Offset reset policy |
| `kafkaMaxPollRecords` | Integer | 500 | Max records per poll |
| `kafkaSessionTimeoutMs` | Integer | 30000 | Session timeout |
| `kafkaHeartbeatIntervalMs` | Integer | 3000 | Heartbeat interval |
| `kafkaEnableAutoCommit` | Boolean | false | Enable auto-commit |
| `kafkaAutoCommitIntervalMs` | Integer | 5000 | Auto-commit interval |

### Appendix B: Migration Checklist

**Pre-Migration:**
- [ ] Review current Pulsar configuration
- [ ] Plan Kafka cluster setup
- [ ] Set up Schema Registry
- [ ] Test Kafka connectivity
- [ ] Backup current configuration

**Migration Steps:**
- [ ] Install Kafka cluster
- [ ] Install Schema Registry
- [ ] Update agent configuration for Kafka
- [ ] Update connector configuration for Kafka
- [ ] Test with sample data
- [ ] Monitor performance
- [ ] Validate data integrity

**Post-Migration:**
- [ ] Verify all data flowing correctly
- [ ] Monitor Kafka metrics
- [ ] Monitor Schema Registry
- [ ] Update documentation
- [ ] Train operations team

### Appendix C: Troubleshooting Guide

**Common Issues:**

1. **Schema Registry Connection Failed**
   - Check Schema Registry URL
   - Verify network connectivity
   - Check Schema Registry logs

2. **Offset Commit Failed**
   - Check consumer group status
   - Verify Kafka broker connectivity
   - Review offset tracking logs

3. **Performance Issues**
   - Review batch size configuration
   - Check compression settings
   - Monitor broker metrics
   - Tune consumer poll settings

4. **Authentication Failed**
   - Verify SASL configuration
   - Check credentials
   - Review Kafka broker security settings

### Appendix D: Performance Tuning Guide

**Producer Tuning:**
- Increase `batch.size` for higher throughput
- Adjust `linger.ms` for batching optimization
- Enable compression for network efficiency
- Use idempotent producer for reliability

**Consumer Tuning:**
- Increase `max.poll.records` for higher throughput
- Adjust `fetch.min.bytes` for batching
- Tune `session.timeout.ms` for stability
- Use sticky assignor for Key_Shared semantics

**Broker Tuning:**
- Increase `num.network.threads`
- Increase `num.io.threads`
- Tune `log.segment.bytes`
- Configure `compression.type`

### Appendix E: Key Files Reference

**New Files:**
- `messaging-kafka/src/main/java/com/datastax/oss/cdc/messaging/kafka/KafkaMessagingClient.java`
- `messaging-kafka/src/main/java/com/datastax/oss/cdc/messaging/kafka/KafkaMessageProducer.java`
- `messaging-kafka/src/main/java/com/datastax/oss/cdc/messaging/kafka/KafkaMessageConsumer.java`
- `messaging-kafka/src/main/java/com/datastax/oss/cdc/messaging/kafka/KafkaSchemaProvider.java`
- `messaging-kafka/src/main/java/com/datastax/oss/cdc/messaging/kafka/KafkaConfigMapper.java`
- `messaging-kafka/src/main/java/com/datastax/oss/cdc/messaging/kafka/KafkaOffsetTracker.java`
- `agent/src/main/java/com/datastax/oss/cdc/agent/AbstractMessagingMutationSender.java`
- `agent/src/main/java/com/datastax/oss/cdc/agent/KafkaMutationSender.java`

**Modified Files:**
- `settings.gradle` - Add messaging-kafka module
- `agent/src/main/java/com/datastax/oss/cdc/agent/AgentConfig.java` - Add Kafka configuration
- `agent/src/main/java/com/datastax/oss/cdc/agent/AbstractPulsarMutationSender.java` - Refactor to use base class
- `connector/src/main/java/com/datastax/oss/cdc/CassandraSourceConnectorConfig.java` - Add Kafka configuration
- `connector/src/main/java/com/datastax/oss/pulsar/source/CassandraSource.java` - Use messaging abstraction

---

## Document End

**Phase 4 Implementation Plan Complete**

This document provides a comprehensive, step-by-step plan for implementing Kafka support in the CDC for Apache Cassandra project. The plan ensures:

1. **No Functionality Breakage**: All existing Pulsar functionality remains intact
2. **DRY Principles**: Maximum reuse of abstractions and base implementations
3. **Build Stability**: All CI jobs pass before and after implementation
4. **Performance Parity**: Kafka implementation meets performance targets
5. **Comprehensive Testing**: ≥90% code coverage with unit, integration, and E2E tests
6. **Complete Documentation**: Configuration guides, migration guides, and troubleshooting

**Next Steps:**
- Review and approve this plan
- Begin Week 1 implementation
- Track progress using daily checkpoints
- Adjust timeline as needed based on actual progress

---

**Document Version:** 1.0  
**Last Updated:** 2026-03-18  
**Status:** Ready for Review