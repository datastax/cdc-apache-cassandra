# CDC Apache Cassandra - Bob Context Summary

## Executive Summary

This document tracks the investigation and resolution of persistent CI failures in the CDC Apache Cassandra project after a major architectural refactoring to introduce a messaging abstraction layer.

## Critical Discovery: Original Root Cause Analysis Was INCORRECT

### What We Thought Was Wrong (INCORRECT)
After 59 failed CI attempts, we believed the root cause was:
- E2E tests in `BackfillCLIE2ETests.java` were never updated during refactoring
- Tests directly instantiated Pulsar clients, bypassing the messaging abstraction
- Production code used the abstraction, tests didn't - architectural mismatch
- Messages sent through abstraction couldn't be received by test consumers

**This analysis was completely wrong.**

### What We Actually Discovered (CORRECT)

**The messaging abstraction layer does NOT support GenericRecord schemas by design.**

#### Architectural Limitation
- `SchemaType` enum in `messaging-api` contains: AVRO, JSON, PROTOBUF, STRING, BYTES, KEY_VALUE, NONE
- **No AUTO_CONSUME, no AUTO, no GENERIC_RECORD support**
- GenericRecord is Pulsar-specific and cannot be abstracted across Kafka
- The abstraction was intentionally designed for typed schemas only

#### Why E2E Tests MUST Use Direct Pulsar Client
1. **Schema-less consumption**: Tests need `Schema.AUTO_CONSUME()` to handle any schema type
2. **Interoperability verification**: Tests verify that messages sent through abstraction can be consumed by standard Pulsar consumers
3. **End-to-end validation**: Production sends typed AVRO → tests consume with AUTO_CONSUME → validates compatibility
4. **Correct pattern**: Production uses abstraction (typed), tests use direct client (schema-less)

#### The Test Implementation Was Correct All Along
```java
// This is the CORRECT pattern for E2E tests
try (PulsarClient pulsarClient = PulsarClient.builder()
        .serviceUrl(pulsarContainer.getPulsarBrokerUrl())
        .build()) {
    try (Consumer<GenericRecord> consumer = pulsarClient.newConsumer(Schema.AUTO_CONSUME())
            .topic(topicName)
            .subscriptionName("sub1")
            .subscriptionType(SubscriptionType.Key_Shared)
            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
            .subscribe()) {
        // Consume and validate messages
    }
}
```

### Actions Taken

#### Phase 1: Reverted Test File (COMPLETED)
- **File**: `backfill-cli/src/test/java/com/datastax/oss/cdc/backfill/e2e/BackfillCLIE2ETests.java`
- **Action**: Fully reverted to original direct Pulsar client pattern
- **Rationale**: The original implementation was architecturally correct
- **Status**: Tests now correctly use `PulsarClient` with `Schema.AUTO_CONSUME()`

#### Phase 2: Cleaned Up Build Dependencies (COMPLETED)
- **File**: `backfill-cli/build.gradle`
- **Action**: Removed unnecessary messaging abstraction test dependencies (lines 97-99)
- **Removed**:
  - `testImplementation project(':messaging-api')`
  - `testImplementation project(':messaging-pulsar')`
  - `testImplementation project(':messaging-kafka')`
- **Rationale**: E2E tests don't use the abstraction, so these dependencies are misleading
- **Status**: Build file now accurately reflects test dependencies

#### Phase 3: Documentation Updated (COMPLETED)
- **File**: `docs/BOB_CONTEXT_SUMMARY.md` (this file)
- **Action**: Documented the incorrect analysis and corrected understanding
- **Status**: Complete

## Actual CI Failure Root Cause: STILL UNKNOWN

### What We Know Now
1. The test implementation pattern is correct
2. The architectural design is sound
3. The original 59-attempt analysis was based on a misunderstanding
4. The real CI failure cause has not been identified

### What Needs Investigation
The actual CI failures must be caused by:
1. **Timing issues**: Async backfill completion, message delivery delays
2. **Container readiness**: Pulsar/Cassandra startup timing in CI environment
3. **Network connectivity**: CI environment network issues
4. **Race conditions**: Async message handling in backfill thread
5. **Test assertions**: Expecting 100 and 1 messages - are these correct?
6. **Production code bugs**: Issues in `PulsarImporter` or `PulsarMutationSenderFactory`
7. **SPI provider discovery**: Runtime provider loading in backfill-cli

### Next Steps Required
1. **Obtain actual CI failure logs** - Review without preconceptions
2. **Run tests locally** - Verify current implementation compiles and passes
3. **Investigate timing** - Check 90s backfill timeout, 30s no-more-messages timeout
4. **Validate production code** - Ensure `PulsarImporter` correctly uses messaging abstraction
5. **Check SPI loading** - Verify providers are discovered at runtime in backfill-cli

## Key Lessons Learned

### 1. Understand Architectural Design Constraints
- The messaging abstraction has intentional limitations
- Not all Pulsar features can be abstracted (GenericRecord, AUTO_CONSUME)
- Design decisions have valid reasons - investigate before assuming bugs

### 2. Production vs Test Patterns Are Different
- Production code should use abstractions for flexibility
- Test code may need direct clients for verification
- E2E tests validate interoperability, not just internal consistency

### 3. Validate Assumptions Through Implementation
- The original analysis seemed logical but was wrong
- Attempting implementation revealed the architectural limitation
- Always verify theories by trying to implement them

### 4. Don't Assume Root Cause Without Evidence
- 59 attempts led to an incorrect conclusion
- The real issue was never investigated with actual CI logs
- Fresh investigation needed without preconceptions

## Technical Architecture

### Messaging Abstraction Layer
- **Purpose**: Provider-agnostic messaging supporting Pulsar and Kafka
- **Components**: MessagingClient, MessageProducer, MessageConsumer
- **SPI**: Service Provider Interface for provider discovery
- **Supported Schemas**: AVRO, JSON, PROTOBUF, STRING, BYTES, KEY_VALUE, NONE
- **NOT Supported**: GenericRecord, AUTO_CONSUME, AUTO (by design)

### Production Code Pattern (CORRECT)
```java
// PulsarMutationSenderFactory.java
MessagingClient client = MessagingClientFactory.create(config);
MessageProducer producer = client.createProducer(producerConfig);
producer.send(message); // Sends typed AVRO messages
```

### Test Code Pattern (CORRECT)
```java
// BackfillCLIE2ETests.java
PulsarClient client = PulsarClient.builder().serviceUrl(url).build();
Consumer<GenericRecord> consumer = client.newConsumer(Schema.AUTO_CONSUME())
    .topic(topic).subscribe();
GenericRecord record = consumer.receive().getValue(); // Receives any schema
```

### Why Both Patterns Are Necessary
1. **Production**: Uses abstraction for provider flexibility (Pulsar or Kafka)
2. **Tests**: Use direct client to verify standard Pulsar consumers can receive messages
3. **Validation**: Ensures abstraction produces Pulsar-compatible messages
4. **Interoperability**: Critical for real-world deployments

## File Changes Summary

### Modified Files
1. **backfill-cli/src/test/java/com/datastax/oss/cdc/backfill/e2e/BackfillCLIE2ETests.java**
   - Reverted to direct Pulsar client pattern
   - Uses `Schema.AUTO_CONSUME()` for GenericRecord consumption
   - Validates messages sent through abstraction

2. **backfill-cli/build.gradle**
   - Removed messaging-api test dependency
   - Removed messaging-pulsar test dependency
   - Removed messaging-kafka test dependency
   - Kept only necessary Pulsar client dependencies

3. **docs/BOB_CONTEXT_SUMMARY.md** (this file)
   - Documented incorrect analysis
   - Explained architectural limitation
   - Outlined next investigation steps

### Unchanged Files (Correctly Implemented)
1. **backfill-cli/src/main/java/com/datastax/oss/cdc/backfill/importer/PulsarMutationSenderFactory.java**
   - Correctly uses `MessagingClientFactory.create()`
   - Properly integrated with messaging abstraction

2. **backfill-cli/src/main/java/com/datastax/oss/cdc/backfill/importer/PulsarImporter.java**
   - Correctly uses factory pattern
   - Sends typed AVRO messages through abstraction

## Current Status

### Completed
- ✅ Reverted E2E tests to correct direct Pulsar client pattern
- ✅ Cleaned up unnecessary test dependencies
- ✅ Documented architectural limitation and corrected analysis
- ✅ Identified that original root cause analysis was incorrect

### Pending
- ⏳ Obtain and review actual CI failure logs
- ⏳ Run tests locally to verify compilation and execution
- ⏳ Investigate actual timing/container/network issues
- ⏳ Validate production code message sending
- ⏳ Check SPI provider discovery at runtime

### Blocked
- 🚫 Cannot proceed with CI fix until actual root cause is identified
- 🚫 Need real CI logs to understand failure mode
- 🚫 Previous 59 attempts were based on incorrect theory

## Conclusion

The 59-attempt investigation led to an incorrect conclusion based on a misunderstanding of the architectural design. The E2E tests were correctly implemented from the start. The messaging abstraction has intentional limitations (no GenericRecord support) that make direct Pulsar client usage necessary for E2E testing.

**The actual CI failure cause remains unknown and requires fresh investigation with actual CI logs.**

---
*Last Updated: 2026-04-06*
*Status: Root cause analysis corrected, awaiting actual CI logs for real investigation*
