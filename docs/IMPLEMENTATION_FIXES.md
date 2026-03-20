# CI Failure Implementation Fixes

**Date**: 2026-03-20  
**Related**: CI_FAILURE_RECOVERY_PLAN.md

## Quick Reference: What to Fix

### Priority 1: CRITICAL - Connector Test Failures (IMMEDIATE)

**Issue**: 24 connector tests failing with `expected: <1> but was: <null>`

**Files to Fix**:
1. `testcontainers/build.gradle` - Dependency isolation
2. Potentially test timing issues

### Priority 2: HIGH - Kafka Test Implementation (2-3 hours)

**Issue**: Kafka tests exist but may have implementation issues

**Files to Check**:
1. `agent-c4/src/test/java/com/datastax/oss/cdc/agent/KafkaSingleNodeC4Tests.java`
2. `testcontainers/src/main/java/com/datastax/oss/cdc/KafkaSingleNodeTests.java`
3. `testcontainers/src/main/java/com/datastax/testcontainers/cassandra/CassandraContainer.java`

### Priority 3: MEDIUM - CI Resource Optimization (1-2 hours)

**Issue**: Jobs timing out after 6 hours

**Files to Fix**:
1. `.github/workflows/ci.yaml` - Add timeouts and fail-fast
2. `.github/workflows/backfill-ci.yaml` - Same optimizations

## Detailed Fix Instructions

### Fix 1: Isolate Kafka Dependencies in testcontainers/build.gradle

**Problem**: Kafka dependencies may be conflicting with Pulsar tests

**Current Code** (lines 36-39):
```gradle
implementation "org.testcontainers:kafka:${testContainersVersion}"
implementation("org.apache.kafka:kafka-clients:3.6.1")
```

**Proposed Fix**:
```gradle
// Keep Kafka dependencies but ensure they don't interfere with Pulsar
testImplementation "org.testcontainers:kafka:${testContainersVersion}"
testImplementation("org.apache.kafka:kafka-clients:3.6.1")
```

**Rationale**: 
- Change from `implementation` to `testImplementation` 
- This ensures Kafka deps are only available during test compilation/runtime
- Prevents potential classpath conflicts with Pulsar

**Alternative Fix** (if above doesn't work):
Create separate source sets for Kafka tests to completely isolate them.

### Fix 2: Add Proper Test Isolation

**Problem**: Kafka and Pulsar tests may be interfering with each other

**Solution**: Ensure test classes are properly isolated

**File**: `testcontainers/src/main/java/com/datastax/oss/cdc/KafkaSingleNodeTests.java`

**Check**:
1. Verify `@BeforeAll` and `@AfterAll` properly clean up resources
2. Ensure Kafka container doesn't conflict with Pulsar container
3. Add proper network isolation

**Current Implementation** (lines 67-81):
```java
@BeforeAll
public static void initBeforeClass() throws Exception {
    testNetwork = Network.newNetwork();
    kafkaContainer = new KafkaContainer(AgentTestUtil.KAFKA_IMAGE)
            .withNetwork(testNetwork)
            .withNetworkAliases("kafka")
            .withKraft()
            .withStartupTimeout(Duration.ofSeconds(60));
    kafkaContainer.start();
}

@AfterAll
public static void closeAfterAll() {
    kafkaContainer.close();
}
```

**Potential Issue**: Network not being closed. Add:
```java
@AfterAll
public static void closeAfterAll() {
    if (kafkaContainer != null) {
        kafkaContainer.close();
    }
    if (testNetwork != null) {
        testNetwork.close();
    }
}
```

### Fix 3: Optimize CI Workflow Timeouts

**File**: `.github/workflows/ci.yaml`

**Current** (line 41):
```yaml
timeout-minutes: 360  # 6 hours
```

**Proposed**:
```yaml
timeout-minutes: 120  # 2 hours - more reasonable
```

**Current** (line 93):
```yaml
timeout-minutes: 360  # 6 hours
```

**Proposed**:
```yaml
timeout-minutes: 90  # 1.5 hours for Kafka tests
```

**Add fail-fast** (line 43):
```yaml
strategy:
  fail-fast: true  # Stop on first failure to save resources
  matrix:
    module: ['agent', 'agent-c3', 'agent-c4', 'agent-dse4', 'connector']
```

### Fix 4: Investigate Connector Test Failure

**Test**: `testCompoundPk()` in `PulsarCassandraSourceTests.java`

**Failure Location** (line 491):
```java
assertEquals((Integer) 1, mutationTable2.get("1"));  // Returns null instead of 1
```

**Investigation Steps**:

1. **Check if mutations are being sent**:
   - Verify Cassandra agent is starting correctly
   - Check agent logs for errors
   - Verify CDC is enabled on table

2. **Check if Pulsar is receiving messages**:
   - Verify topic creation
   - Check Pulsar logs
   - Verify connector deployment

3. **Check timing issues**:
   - Current timeout: 60 seconds (line 479)
   - May need to increase or add retry logic

**Potential Fix 1**: Increase timeout
```java
// Line 479 - increase from 60 to 90 seconds
while ((msg = consumer.receive(90, TimeUnit.SECONDS)) != null &&
        mutationTable2.values().stream().mapToInt(i -> i).sum() < 4) {
```

**Potential Fix 2**: Add debug logging
```java
// After line 488
log.info("Received mutation for key: {}, current count: {}", 
         getAndAssertKeyFieldAsString(key, "a"), 
         mutationTable2.get(getAndAssertKeyFieldAsString(key, "a")));
```

**Potential Fix 3**: Check container startup
Verify `cassandraContainer1` and `cassandraContainer2` are fully started before running tests.

### Fix 5: Verify Kafka Test Implementation

**File**: `agent-c4/src/test/java/com/datastax/oss/cdc/agent/KafkaSingleNodeC4Tests.java`

**Current Implementation** looks correct, but verify:

1. **Cassandra container creation** (line 41-42):
```java
return CassandraContainer.createCassandraContainerWithAgentKafka(
        CASSANDRA_IMAGE, testNetwork, nodeIndex, "c4", kafkaBootstrapServers);
```

**Check**: Does `createCassandraContainerWithAgentKafka` method exist?

2. **Verify method exists in CassandraContainer.java**:
Need to check if this method was actually implemented.

### Fix 6: Add Missing Kafka Container Helper Method

**File**: `testcontainers/src/main/java/com/datastax/testcontainers/cassandra/CassandraContainer.java`

**Check if method exists**: `createCassandraContainerWithAgentKafka`

If missing, need to add it similar to the Pulsar version:

```java
public static CassandraContainer<?> createCassandraContainerWithAgentKafka(
        DockerImageName cassandraImage,
        Network network,
        int nodeIndex,
        String cassandraVersion,
        String kafkaBootstrapServers) {
    
    String agentBuildDir = System.getProperty("agentBuildDir");
    String agentModule = "agent-" + cassandraVersion;
    
    return new CassandraContainer<>(cassandraImage)
            .withNetwork(network)
            .withCreateContainerCmdModifier(createContainerCmd -> 
                createContainerCmd.withName("cassandra" + nodeIndex))
            .withFileSystemBind(
                    String.format("%s/libs", agentBuildDir),
                    "/usr/local/lib/agent",
                    BindMode.READ_ONLY)
            .withEnv("JVM_EXTRA_OPTS", 
                    "-javaagent:/usr/local/lib/agent/agent-" + cassandraVersion + ".jar=" +
                    "kafkaBootstrapServers=" + kafkaBootstrapServers)
            .withStartupTimeout(Duration.ofSeconds(120));
}
```

## Implementation Order

### Phase 1: Stabilize Existing Tests (CRITICAL)

1. **Fix testcontainers/build.gradle** (5 min)
   - Change Kafka deps to `testImplementation`
   - Rebuild and verify

2. **Add network cleanup** (5 min)
   - Update `KafkaSingleNodeTests.closeAfterAll()`
   - Ensure proper resource cleanup

3. **Run connector tests locally** (30 min)
   - `./gradlew connector:test`
   - Identify specific failure cause
   - Apply appropriate fix

4. **Increase test timeouts if needed** (10 min)
   - Update `testCompoundPk` timeout from 60s to 90s
   - Add debug logging

### Phase 2: Fix Kafka Tests (if needed)

1. **Verify Kafka helper method exists** (10 min)
   - Check `CassandraContainer.java`
   - Add if missing

2. **Run Kafka tests locally** (30 min)
   - `./gradlew agent-c4:test -PtestKafkaImage=apache/kafka -PtestKafkaImageTag=4.2.0`
   - Fix any issues

### Phase 3: Optimize CI

1. **Update timeouts** (10 min)
   - Reduce from 360 to 120 minutes
   - Add fail-fast strategy

2. **Test CI changes** (wait for CI run)
   - Push to feature branch
   - Monitor execution

## Testing Checklist

### Local Testing (Before Pushing)

- [ ] `./gradlew clean build` - Full build succeeds
- [ ] `./gradlew connector:test` - All connector tests pass
- [ ] `./gradlew agent-c4:test` - All agent-c4 Pulsar tests pass
- [ ] `./gradlew agent-c4:test -PtestKafkaImage=apache/kafka -PtestKafkaImageTag=4.2.0` - Kafka tests pass
- [ ] Check for dependency conflicts: `./gradlew dependencies`

### CI Testing (After Pushing)

- [ ] Monitor build job - completes successfully
- [ ] Monitor test jobs - no timeouts
- [ ] Check connector tests - all pass
- [ ] Check agent tests - all pass
- [ ] Check Kafka tests - all pass
- [ ] Verify total CI time < 2 hours

## Rollback Plan

If fixes don't work after 4 hours of effort:

### Option 1: Minimal Rollback (Recommended)
1. Keep Kafka implementation code (messaging-kafka module)
2. Remove Kafka tests temporarily:
   - Delete `KafkaSingleNodeTests.java`
   - Delete `KafkaSingleNodeC4Tests.java`
   - Remove `test-kafka` job from CI
3. Fix connector tests in isolation
4. Re-add Kafka tests later when stable

### Option 2: Full Rollback
1. Revert all Kafka-related commits
2. Verify all tests pass
3. Start over with better planning

## Success Metrics

### Must Have (Phase 1)
- ✅ All connector tests pass (104/104)
- ✅ No `testCompoundPk` failures
- ✅ CI completes without timeouts

### Should Have (Phase 2)
- ✅ Kafka tests pass for agent-c4
- ✅ All 6 Kafka CI jobs green
- ✅ Total CI time < 2 hours

### Nice to Have (Phase 3)
- ✅ CI optimized to < 90 minutes
- ✅ Resource usage reduced
- ✅ Better error reporting

## Next Actions

1. **IMMEDIATE**: Fix `testcontainers/build.gradle` dependency scope
2. **NEXT**: Run connector tests locally to reproduce failure
3. **THEN**: Apply appropriate fix based on findings
4. **FINALLY**: Optimize CI workflows

## Questions to Answer

1. ❓ Does `createCassandraContainerWithAgentKafka` method exist?
2. ❓ Are there any dependency version conflicts?
3. ❓ Is the connector test failure timing-related or logic-related?
4. ❓ Do we need separate test source sets for Kafka?

## Resources Needed

- Local development environment with Docker
- Access to CI logs for detailed error analysis
- Ability to run tests with different configurations
- Time: 7-11 hours total for complete fix