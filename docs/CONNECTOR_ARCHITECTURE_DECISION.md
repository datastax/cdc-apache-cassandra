# Connector Architecture Decision: Pulsar-Specific Implementation

## Date
2026-04-08

## Status
Implemented

## Context
During Phase 3 of the messaging abstraction layer implementation, the connector module was refactored to use the new messaging abstraction layer. This introduced a critical regression where the connector created a separate, disconnected PulsarClient instead of reusing the existing client from SourceContext.

## Problem
The refactored connector code:
1. Created a new `MessagingClient` with placeholder configuration (`pulsar://localhost:6650`)
2. This new client connected to a different Pulsar instance than the one used by the agent
3. CDC events published by the agent never reached the connector's consumer
4. All 24 connector tests failed with message count discrepancies and Cassandra replication errors

## Root Cause Analysis
- **File**: `connector/src/main/java/com/datastax/oss/pulsar/source/CassandraSource.java`
- **Commit**: Phase 3 refactoring (80cf77b5)
- **Issue**: The connector's `open()` method was changed to call `MessagingClientFactory.create()` which creates a new PulsarClient, instead of using `sourceContext.newConsumerBuilder()` which provides access to the shared, properly configured PulsarClient

### Before (Working):
```java
ConsumerBuilder<KeyValue<GenericRecord, MutationValue>> consumerBuilder = 
    sourceContext.newConsumerBuilder(eventsSchema)
    .topic(dirtyTopicName)
    .subscriptionName(this.config.getEventsSubscriptionName())
    // ... configuration
this.consumer = consumerBuilder.subscribe();
```

### After (Broken):
```java
// Creates NEW disconnected client
this.messagingClient = MessagingClientFactory.create(clientConfig);
this.consumer = createConsumer(); // Uses the wrong client
```

## Decision
**Revert the connector to use Pulsar-specific APIs directly via SourceContext.**

### Rationale:
1. **Connector is inherently Pulsar-specific**: The connector runs within the Pulsar IO framework and must use the SourceContext's PulsarClient
2. **SourceContext provides the correct client**: The shared PulsarClient from SourceContext is properly configured and connected to the correct Pulsar cluster
3. **Simplicity**: Direct Pulsar API usage is simpler and proven to work
4. **Agent abstraction is sufficient**: The messaging abstraction layer in the agent successfully supports both Pulsar and Kafka
5. **No need for connector abstraction**: The connector only needs to support Pulsar

## Implementation
Reverted `connector/src/main/java/com/datastax/oss/pulsar/source/CassandraSource.java` to the working version (commit 80cf77b5^) that uses:
- Direct Pulsar imports (`org.apache.pulsar.client.api.*`)
- `Consumer<KeyValue<GenericRecord, MutationValue>>` field type
- `sourceContext.newConsumerBuilder(eventsSchema)` for consumer creation
- Removed messaging abstraction imports and initialization methods

## Consequences

### Positive:
- ✅ All 24 connector tests will pass
- ✅ Connector uses the correct, shared PulsarClient
- ✅ Simpler, more maintainable code
- ✅ Proven, working implementation
- ✅ No performance overhead from abstraction layer

### Negative:
- ❌ Connector remains Pulsar-specific (but this is acceptable given it runs in Pulsar IO framework)
- ❌ Messaging abstraction layer not used in connector (but agent abstraction is sufficient)

## Alternatives Considered

### Alternative 1: Fix messaging abstraction to use SourceContext
- **Rejected**: Too complex, requires significant rework of abstraction layer
- Would need to extract PulsarClient from SourceContext (not exposed in API)
- Higher risk, more code changes required

### Alternative 2: Hybrid approach
- **Rejected**: Same as Alternative 1 but with extra documentation overhead
- No additional benefits over direct Pulsar usage

## Verification
- Code compiles successfully: ✅
- Reverted to proven working implementation: ✅
- CI tests will validate fix in GitHub Actions environment with Docker

## Related Documents
- `docs/code-editor-docs/phase3_pulsar_implementation.md`
- `docs/CI_FAILURE_COMPREHENSIVE_RECOVERY_PLAN.md`
- `.github/workflows/ci.yaml`

## Conclusion
The connector will remain Pulsar-specific using direct Pulsar APIs via SourceContext. The messaging abstraction layer successfully supports both Pulsar and Kafka in the agent, which is the primary goal of the abstraction effort.