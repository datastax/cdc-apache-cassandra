# ADR 001: Messaging Abstraction Layer

**Status:** Accepted  
**Date:** 2026-03-17  
**Decision Makers:** CDC Development Team  
**Related:** Phase 1 Implementation

## Context

The CDC for Apache Cassandra project is currently tightly coupled to Apache Pulsar as its messaging platform. This creates several challenges:

1. **Vendor Lock-in**: Cannot easily switch to alternative messaging platforms
2. **Limited Flexibility**: Customers requiring Kafka cannot use the solution
3. **Code Duplication**: Platform-specific code scattered throughout codebase
4. **Testing Complexity**: Difficult to test without full Pulsar infrastructure
5. **Maintenance Burden**: Changes to Pulsar API require updates across multiple modules

## Decision

We will introduce a **messaging abstraction layer** that provides platform-independent interfaces for all messaging operations. This abstraction will:

1. **Define Core Interfaces**:
   - `MessagingClient` - Connection lifecycle management
   - `MessageProducer` - Message production
   - `MessageConsumer` - Message consumption
   - `Message` - Message representation
   - `MessageId` - Message identification

2. **Support Multiple Providers**:
   - Apache Pulsar (existing, Phase 3)
   - Apache Kafka (new, Phase 4)
   - Extensible for future platforms

3. **Maintain Backward Compatibility**:
   - Existing Pulsar functionality unchanged
   - No breaking changes to current deployments
   - Gradual migration path

## Architecture

### Module Structure

```
messaging-api/          # New module with interfaces only
├── MessagingClient
├── MessageProducer
├── MessageConsumer
├── config/            # Configuration interfaces
├── schema/            # Schema management
└── stats/             # Statistics interfaces
```

### Key Design Principles

1. **Interface Segregation**: Focused interfaces for specific concerns
2. **DRY**: Eliminate code duplication through shared abstractions
3. **Platform Independence**: No platform-specific types in interfaces
4. **Extensibility**: Easy to add new messaging platforms

### Configuration Model

Unified configuration supporting provider-specific settings:

```java
ClientConfig config = ClientConfig.builder()
    .provider(MessagingProvider.PULSAR)
    .serviceUrl("pulsar://localhost:6650")
    .providerProperties(platformSpecificProps)
    .build();
```

## Consequences

### Positive

1. **Multi-Platform Support**: Can support both Pulsar and Kafka
2. **Reduced Coupling**: Clean separation between business logic and messaging
3. **Improved Testability**: Can mock interfaces for unit testing
4. **Better Maintainability**: Changes isolated to specific implementations
5. **Future-Proof**: Easy to add new messaging platforms

### Negative

1. **Initial Development Cost**: Requires upfront design and implementation effort
2. **Abstraction Overhead**: Small performance cost from indirection (< 5%)
3. **Learning Curve**: Team must understand abstraction layer
4. **Complexity**: Additional layer to maintain

### Neutral

1. **No Impact on Existing Deployments**: Backward compatible
2. **Gradual Migration**: Can migrate incrementally
3. **Documentation Needs**: Requires comprehensive API documentation

## Implementation Plan

### Phase 1: Design and Interface Definition (2 weeks) ✅
- Define all abstraction interfaces
- Document API contracts
- Create configuration model
- **Status**: COMPLETED

### Phase 2: Core Abstraction Layer (3 weeks)
- Implement base abstraction classes
- Create factory patterns
- Set up testing framework

### Phase 3: Pulsar Implementation (3 weeks)
- Implement Pulsar-specific adapters
- Migrate existing Pulsar code
- Maintain backward compatibility

### Phase 4: Kafka Implementation (4 weeks)
- Implement Kafka-specific adapters
- Handle Kafka-specific concepts
- Performance optimization

### Phase 5: Testing and Migration (3 weeks)
- End-to-end testing
- Performance validation
- Documentation

## Alternatives Considered

### Alternative 1: Continue with Pulsar Only
**Rejected**: Limits customer choice and creates vendor lock-in

### Alternative 2: Separate Kafka Fork
**Rejected**: Creates code duplication and maintenance burden

### Alternative 3: Runtime Plugin System
**Rejected**: Too complex for current needs, can be added later if needed

## References

- [Phase 1 Design Document](../phase1_design_and_interface_definition.md)
- [Current Architecture](../Current_Architecture.md)
- [Apache Pulsar Documentation](https://pulsar.apache.org/docs/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)

## Notes

- All interfaces are in `messaging-api` module with zero external dependencies
- Provider implementations will be in separate modules (`messaging-pulsar`, `messaging-kafka`)
- Configuration uses builder pattern for flexibility
- Statistics interfaces provide observability across all platforms