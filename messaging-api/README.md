# Messaging API

**Version:** 1.0.0  
**Status:** Phase 1 - Interface Definition Complete

## Overview

The Messaging API provides a platform-agnostic abstraction layer for messaging operations in the CDC for Apache Cassandra project. It enables support for multiple messaging platforms (Apache Pulsar, Apache Kafka) through a unified interface.

## Design Principles

- **Platform Independence**: Clean abstraction over messaging platform specifics
- **DRY (Don't Repeat Yourself)**: Shared interfaces eliminate code duplication
- **Interface Segregation**: Focused interfaces for specific concerns
- **Backward Compatibility**: Existing Pulsar functionality remains intact

## Architecture

```
messaging-api/
├── src/main/java/com/datastax/oss/cdc/messaging/
│   ├── MessagingClient.java          # Client lifecycle management
│   ├── MessageProducer.java          # Message production
│   ├── MessageConsumer.java          # Message consumption
│   ├── Message.java                  # Message representation
│   ├── MessageId.java                # Message identifier
│   ├── MessagingException.java       # Base exception
│   ├── ConnectionException.java      # Connection errors
│   ├── ProducerException.java        # Producer errors
│   ├── ConsumerException.java        # Consumer errors
│   ├── config/                       # Configuration interfaces
│   │   ├── ClientConfig.java
│   │   ├── ProducerConfig.java
│   │   ├── ConsumerConfig.java
│   │   ├── AuthConfig.java
│   │   ├── SslConfig.java
│   │   ├── BatchConfig.java
│   │   ├── RoutingConfig.java
│   │   ├── MessagingProvider.java
│   │   ├── SubscriptionType.java
│   │   ├── InitialPosition.java
│   │   └── CompressionType.java
│   ├── schema/                       # Schema management
│   │   ├── SchemaProvider.java
│   │   ├── SchemaDefinition.java
│   │   ├── SchemaInfo.java
│   │   ├── SchemaType.java
│   │   └── SchemaException.java
│   └── stats/                        # Statistics
│       ├── ClientStats.java
│       ├── ProducerStats.java
│       └── ConsumerStats.java
```

## Core Interfaces

### MessagingClient

Entry point for all messaging operations. Manages connection lifecycle and creates producers/consumers.

```java
MessagingClient client = MessagingClientFactory.create(config);
client.initialize(config);

MessageProducer<String, MyData> producer = client.createProducer(producerConfig);
MessageConsumer<String, MyData> consumer = client.createConsumer(consumerConfig);

client.close();
```

### MessageProducer

Publishes messages to topics with key-value pairs and properties.

```java
Map<String, String> properties = Map.of("token", "12345");
MessageId id = producer.send(key, value, properties);

// Async
CompletableFuture<MessageId> future = producer.sendAsync(key, value, properties);
```

### MessageConsumer

Consumes messages from topics with acknowledgment support.

```java
Message<String, MyData> msg = consumer.receive(Duration.ofSeconds(1));
if (msg != null) {
    process(msg);
    consumer.acknowledge(msg);
}
```

## Configuration

### Client Configuration

```java
ClientConfig config = ClientConfig.builder()
    .provider(MessagingProvider.PULSAR)
    .serviceUrl("pulsar://localhost:6650")
    .authConfig(authConfig)
    .sslConfig(sslConfig)
    .memoryLimitBytes(1024 * 1024 * 1024)
    .build();
```

### Producer Configuration

```java
ProducerConfig<String, MyData> config = ProducerConfig.builder()
    .topic("my-topic")
    .keySchema(keySchema)
    .valueSchema(valueSchema)
    .batchConfig(batchConfig)
    .maxPendingMessages(1000)
    .build();
```

### Consumer Configuration

```java
ConsumerConfig<String, MyData> config = ConsumerConfig.builder()
    .topic("my-topic")
    .subscriptionName("my-subscription")
    .subscriptionType(SubscriptionType.KEY_SHARED)
    .keySchema(keySchema)
    .valueSchema(valueSchema)
    .initialPosition(InitialPosition.EARLIEST)
    .build();
```

## Schema Management

### Schema Definition

```java
SchemaDefinition schema = SchemaDefinition.builder()
    .type(SchemaType.AVRO)
    .schemaDefinition(avroSchemaJson)
    .name("MySchema")
    .build();
```

### Schema Provider

```java
SchemaProvider provider = client.getSchemaProvider();
SchemaInfo info = provider.registerSchema("my-topic", schema);
Optional<SchemaInfo> retrieved = provider.getSchema("my-topic");
```

## Statistics

### Producer Statistics

```java
ProducerStats stats = producer.getStats();
long sent = stats.getMessagesSent();
double latency = stats.getAverageSendLatencyMs();
```

### Consumer Statistics

```java
ConsumerStats stats = consumer.getStats();
long received = stats.getMessagesReceived();
long acks = stats.getAcknowledgments();
```

## Exception Handling

```java
try {
    MessageId id = producer.send(key, value, properties);
} catch (ProducerException e) {
    // Handle producer errors
} catch (ConnectionException e) {
    // Handle connection errors
} catch (MessagingException e) {
    // Handle general messaging errors
}
```

## Platform Support

### Supported Platforms

- **Apache Pulsar** (2.8.1+)
- **Apache Kafka** (Planned for Phase 4)

### Provider-Specific Properties

Use `getProviderProperties()` for platform-specific configuration:

```java
Map<String, Object> props = Map.of(
    "pulsar.specific.property", "value",
    "kafka.specific.property", "value"
);
```

## Thread Safety

- **MessagingClient**: Thread-safe, can be shared
- **MessageProducer**: Thread-safe, supports concurrent sends
- **MessageConsumer**: Not thread-safe, use one per thread
- **Configuration**: Immutable, thread-safe

## Performance Considerations

### Batching

Enable batching for higher throughput:

```java
BatchConfig batch = BatchConfig.builder()
    .enabled(true)
    .maxMessages(100)
    .maxDelayMs(10)
    .build();
```

### Async Operations

Use async methods for non-blocking operations:

```java
CompletableFuture<MessageId> future = producer.sendAsync(key, value, props);
future.thenAccept(id -> System.out.println("Sent: " + id));
```

## Migration Guide

### From Direct Pulsar API

**Before:**
```java
PulsarClient client = PulsarClient.builder()
    .serviceUrl("pulsar://localhost:6650")
    .build();
Producer<byte[]> producer = client.newProducer()
    .topic("my-topic")
    .create();
```

**After:**
```java
MessagingClient client = MessagingClientFactory.create(config);
client.initialize(config);
MessageProducer<K, V> producer = client.createProducer(producerConfig);
```

## Testing

### Unit Tests

```java
@Test
void testProducerSend() throws MessagingException {
    MessageProducer<String, String> producer = createTestProducer();
    MessageId id = producer.send("key", "value", Map.of());
    assertNotNull(id);
}
```

### Integration Tests

Integration tests will be added in Phase 3 (Pulsar Implementation) and Phase 4 (Kafka Implementation).

## Future Enhancements

- **Phase 2**: Core abstraction layer implementation
- **Phase 3**: Pulsar adapter implementation
- **Phase 4**: Kafka adapter implementation
- **Phase 5**: End-to-end testing and migration

## References

- [Phase 1 Design Document](../docs/phase1_design_and_interface_definition.md)
- [Current Architecture](../docs/Current_Architecture.md)
- [Apache Pulsar Documentation](https://pulsar.apache.org/docs/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)

## License

Copyright DataStax, Inc.

Licensed under the Apache License, Version 2.0.