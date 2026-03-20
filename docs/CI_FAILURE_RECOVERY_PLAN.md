# CI Test Failure Recovery Plan

**Date**: 2026-03-20  
**Status**: CRITICAL - 31 Errors, 23 Warnings  
**Affected Jobs**: 37 out of 51 CI jobs failing

## Executive Summary

The Kafka integration work introduced **three critical issues** that broke the CI pipeline:

1. **Kafka tests running against unsupported modules** (agent-c3, agent-dse4, connector)
2. **Resource exhaustion** from 51 parallel CI jobs causing timeouts
3. **Existing Pulsar connector tests broken** (24 test failures)

## Root Cause Analysis

### Issue 1: Overly Broad Kafka Test Matrix ❌

**Problem**: CI workflow runs Kafka tests for modules without Kafka support

**Evidence**:
```yaml
# .github/workflows/ci.yaml lines 89-139
test-kafka:
  matrix:
    module: ['agent-c4']  # ✅ Correct - only agent-c4
    jdk: ['11', '17']
    kafkaImage: ['apache/kafka:4.2.0', 'confluentinc/cp-kafka:7.9.6', 'confluentinc/cp-kafka:8.1.0']
```

**Actual Behavior**: Tests fail because:
- agent-c3, agent-dse4 don't have `KafkaSingleNodeC3Tests.java` or `KafkaSingleNodeDse4Tests.java`
- connector module doesn't have Kafka support implemented

**Impact**: 6 Kafka test jobs failing (expected - no implementation exists)

### Issue 2: Resource Exhaustion ⚠️

**Problem**: Too many parallel CI jobs overwhelming GitHub runners

**Evidence**:
- 13 jobs exceeded 6-hour timeout
- "Lost communication with server" errors
- "Java heap space" errors
- Jobs: agent-c3, agent-c4, agent-dse4, agent-dse4 with various Pulsar versions

**CI Job Count**:
- Pulsar tests: 45 jobs (5 modules × 2 JDKs × 3 Pulsar versions + connector variations)
- Kafka tests: 6 jobs (1 module × 2 JDKs × 3 Kafka versions)
- **Total: 51 parallel jobs**

**Impact**: Resource starvation causing legitimate tests to timeout

### Issue 3: Connector Test Failures 🔴

**Problem**: Existing Pulsar connector tests broken

**Evidence**:
```
Test (connector, 11, datastax/lunastreaming:2.10_3.4)
AvroKeyValueCassandraSourceTests > testCompoundPk() FAILED
    org.opentest4j.AssertionFailedError: expected: <1> but was: <null>
    
104 tests completed, 24 failed
```

**Potential Causes**:
1. Kafka dependency conflicts in `testcontainers/build.gradle`
2. Test infrastructure changes affecting Pulsar tests
3. Timing issues from resource contention

**Impact**: Core CDC functionality tests failing

## Recovery Plan

### Phase 1: IMMEDIATE STABILIZATION (2-3 hours)

**Goal**: Stop the bleeding - get existing Pulsar tests passing

#### Step 1.1: Isolate Kafka Dependencies (30 min)
- [ ] Move Kafka dependencies to separate test source set
- [ ] Ensure Kafka deps don't affect Pulsar tests
- [ ] Verify connector tests pass without Kafka deps

#### Step 1.2: Fix CI Workflow (30 min)
- [ ] Ensure `test-kafka` job only runs for agent-c4
- [ ] Add conditional execution to prevent running on unsupported modules
- [ ] Reduce parallel job count if needed

#### Step 1.3: Investigate Connector Failures (1-2 hours)
- [ ] Run connector tests locally to reproduce
- [ ] Check for dependency conflicts
- [ ] Review recent changes to test infrastructure
- [ ] Fix `testCompoundPk` assertion failure

### Phase 2: KAFKA TEST FIXES (2-3 hours)

**Goal**: Get Kafka tests working for agent-c4

#### Step 2.1: Fix Kafka Test Implementation (1 hour)
- [ ] Review `KafkaSingleNodeC4Tests.java` implementation
- [ ] Verify Kafka container setup
- [ ] Check agent configuration for Kafka
- [ ] Ensure proper topic creation and consumption

#### Step 2.2: Validate Kafka Tests Locally (1 hour)
- [ ] Run Kafka tests with all 3 Kafka versions
- [ ] Test with both JDK 11 and 17
- [ ] Verify CDC events flow correctly

### Phase 3: RESOURCE OPTIMIZATION (1-2 hours)

**Goal**: Prevent resource exhaustion

#### Step 3.1: Optimize CI Matrix (30 min)
- [ ] Consider reducing Pulsar version matrix (test only 2 versions instead of 3)
- [ ] Stagger job execution if needed
- [ ] Add resource limits to prevent runaway jobs

#### Step 3.2: Add Fail-Fast Strategy (30 min)
- [ ] Enable fail-fast for critical test failures
- [ ] Add timeout guards (2 hours max per job)
- [ ] Implement better error reporting

### Phase 4: VALIDATION (2-3 hours)

**Goal**: Ensure all tests pass

#### Step 4.1: Local Testing
- [ ] Run full test suite locally for agent-c4
- [ ] Run connector tests
- [ ] Run Kafka tests
- [ ] Verify no regressions

#### Step 4.2: CI Validation
- [ ] Push fixes to feature branch
- [ ] Monitor CI execution
- [ ] Verify all jobs complete successfully
- [ ] Check resource usage

## Detailed Fix Specifications

### Fix 1: Isolate Kafka Dependencies

**File**: `testcontainers/build.gradle`

**Current (Problematic)**:
```gradle
dependencies {
    implementation "org.testcontainers:kafka:${testContainersVersion}"
    implementation("org.apache.kafka:kafka-clients:3.6.1")
}
```

**Proposed Fix**:
```gradle
dependencies {
    // Pulsar dependencies (always available)
    implementation("${pulsarGroup}:pulsar-client:${pulsarVersion}")
    
    // Kafka dependencies (separate configuration)
    testImplementation "org.testcontainers:kafka:${testContainersVersion}"
    testImplementation("org.apache.kafka:kafka-clients:3.6.1")
}
```

**Rationale**: Keep Kafka deps in test scope to avoid affecting production code

### Fix 2: Correct CI Workflow

**File**: `.github/workflows/ci.yaml`

**Current (Correct)**:
```yaml
test-kafka:
  needs: build
  name: Test Kafka
  runs-on: ubuntu-latest
  timeout-minutes: 360
  strategy:
    fail-fast: false
    matrix:
      module: ['agent-c4']  # ✅ Already correct!
      jdk: ['11', '17']
      kafkaImage: ['apache/kafka:4.2.0', 'confluentinc/cp-kafka:7.9.6', 'confluentinc/cp-kafka:8.1.0']
```

**Issue**: The workflow is already correct! The problem is that Kafka tests don't exist yet.

**Proposed Enhancement**:
```yaml
test-kafka:
  needs: build
  name: Test Kafka
  runs-on: ubuntu-latest
  timeout-minutes: 120  # Reduce from 360 to 120
  strategy:
    fail-fast: true  # Change to true to stop on first failure
    matrix:
      module: ['agent-c4']
      jdk: ['11', '17']
      kafkaImage: ['apache/kafka:4.2.0', 'confluentinc/cp-kafka:7.9.6', 'confluentinc/cp-kafka:8.1.0']
```

### Fix 3: Connector Test Investigation

**Test**: `AvroKeyValueCassandraSourceTests.testCompoundPk()`

**Failure**:
```
expected: <1> but was: <null>
```

**Location**: Line 491 in `PulsarCassandraSourceTests.java`
```java
assertEquals((Integer) 1, mutationTable2.get("1"));  // Failing here
```

**Hypothesis**: The mutation is not being received or processed correctly

**Investigation Steps**:
1. Check if Cassandra agent is starting correctly
2. Verify Pulsar topic creation
3. Check for timing issues (messages not arriving in time)
4. Review recent changes to `CassandraContainer.createCassandraContainerWithAgent()`

### Fix 4: Resource Optimization

**Current CI Jobs**: 51 total
- Pulsar: 5 modules × 2 JDKs × 3 versions = 30 jobs
- Connector: 2 JDKs × 3 versions = 6 jobs  
- Kafka: 1 module × 2 JDKs × 3 versions = 6 jobs
- Build: 1 job
- Backfill: 3 families × 3 versions × 1 JDK = 9 jobs

**Proposed Optimization**:
1. Reduce Pulsar versions from 3 to 2 (remove oldest)
2. Add timeout of 120 minutes (2 hours) instead of 360 (6 hours)
3. Enable fail-fast for non-matrix jobs

**Expected Reduction**: 51 → 35 jobs (~30% reduction)

## Success Criteria

### Phase 1 Success
- [ ] All connector tests pass (104/104)
- [ ] No `testCompoundPk` failures
- [ ] Pulsar tests stable

### Phase 2 Success
- [ ] Kafka tests pass for agent-c4
- [ ] All 6 Kafka CI jobs green
- [ ] CDC events flow correctly to Kafka

### Phase 3 Success
- [ ] No jobs timeout
- [ ] No resource exhaustion errors
- [ ] CI completes in < 90 minutes

### Phase 4 Success
- [ ] All 51 (or optimized count) CI jobs pass
- [ ] No flaky tests
- [ ] Documentation updated

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Connector tests still fail after fixes | Medium | High | Thorough local testing before CI |
| Kafka tests reveal deeper issues | Medium | Medium | Incremental implementation |
| Resource issues persist | Low | High | Further matrix reduction |
| Dependency conflicts | Low | Medium | Careful dependency management |

## Rollback Strategy

If fixes don't work within 8 hours:

1. **Revert Kafka Integration**:
   - Remove `test-kafka` job from CI
   - Remove Kafka dependencies from `testcontainers/build.gradle`
   - Remove `KafkaSingleNodeTests.java` and `KafkaSingleNodeC4Tests.java`
   - Remove Kafka properties from `gradle.properties`

2. **Restore Stability**:
   - Verify all Pulsar tests pass
   - Confirm connector tests work
   - Document what went wrong

3. **Plan Better Approach**:
   - Implement Kafka in separate branch
   - Add comprehensive local testing
   - Gradual CI integration

## Timeline

| Phase | Duration | Start | End |
|-------|----------|-------|-----|
| Phase 1: Stabilization | 2-3 hours | Immediate | +3 hours |
| Phase 2: Kafka Fixes | 2-3 hours | +3 hours | +6 hours |
| Phase 3: Optimization | 1-2 hours | +6 hours | +8 hours |
| Phase 4: Validation | 2-3 hours | +8 hours | +11 hours |
| **Total** | **7-11 hours** | Now | +11 hours |

## Next Steps

1. **Immediate**: Start Phase 1 - Stabilize connector tests
2. **Review**: Get stakeholder approval for approach
3. **Execute**: Follow recovery plan systematically
4. **Monitor**: Track progress and adjust as needed
5. **Document**: Update BOB_CONTEXT_SUMMARY.md with lessons learned

## Lessons Learned (To Be Updated)

### What Went Wrong
1. Added Kafka tests without verifying existing tests still pass
2. Didn't test dependency changes locally before CI
3. Underestimated CI resource requirements
4. Insufficient test isolation between Pulsar and Kafka

### Prevention Strategies
1. **Always run full test suite locally before pushing**
2. **Add pre-commit hooks for critical tests**
3. **Implement gradual CI rollout** (feature flags for new test suites)
4. **Monitor CI resource usage** and set appropriate limits
5. **Better dependency isolation** (separate test configurations)

## Contact & Escalation

- **Primary**: Bob (AI Assistant)
- **Escalation**: Project maintainers
- **Timeline**: If not resolved in 11 hours, escalate for manual intervention