#!/usr/bin/env bash
# Step 6 (optional, run after run-all.sh): exercises the Schema Registry integration's actual
# reason for existing — versioning the data topic's Avro schema across Cassandra ALTER TABLEs,
# instead of the old behavior of silently swapping the schema in place with no compatibility
# check. Two scenarios:
#   1. Compatible evolution: ALTER TABLE adds a nullable column -> a new schema version
#      registers cleanly under the same subject (Cassandra's ADD COLUMN is always nullable,
#      which Avro's schema builder maps to a nullable union with a null default -> BACKWARD
#      compatible under the registry's default compatibility mode).
#   2. Auto-register disabled: flips schema.registry.autoRegisterSchemas=false, then forces
#      another schema change. Because nothing pre-registered the new schema, the registry
#      rejects it (RestClientException) and the connector is left retrying that row forever
#      (see KafkaCassandraSourceTask#waitForCqlWithRetry) rather than crashing -- proving the
#      flag actually blocks something instead of always silently succeeding.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

fail=0
check() {
  local desc="$1"; shift
  if "$@" >/dev/null 2>&1; then
    log "PASS: ${desc}"
  else
    log "FAIL: ${desc}"
    fail=1
  fi
}

subject_version_count() {
  curl -sf "${SCHEMA_REGISTRY_URL}/subjects/${DATA_TOPIC}-value/versions" | tr -d '[]' | tr ',' '\n' | grep -c .
}

data_topic_offset() {
  podman exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "${DATA_TOPIC}" | cut -d: -f3
}

# Reads the current connector config back from the REST API (the source of truth once
# reconfigured), tweaks one key with $2, and PUTs it back.
reconfigure_connector() {
  local key="$1" value="$2"
  python3 -c "
import json, urllib.request
req = urllib.request.urlopen('http://localhost:8083/connectors/${CONNECTOR_NAME}/config')
config = json.load(req)
config.pop('name', None)
config['${key}'] = '${value}'
body = json.dumps(config).encode()
put = urllib.request.Request(
    'http://localhost:8083/connectors/${CONNECTOR_NAME}/config',
    data=body, method='PUT', headers={'Content-Type': 'application/json'})
urllib.request.urlopen(put)
"
  wait_for "Connector RUNNING after reconfigure (${key}=${value})" bash -c \
    "curl -sf http://localhost:8083/connectors/${CONNECTOR_NAME}/status | grep -q '\"state\":\"RUNNING\"'"
}

log "Schema versions for ${DATA_TOPIC}-value before evolution: $(subject_version_count)"

log "--- Scenario 1: compatible evolution (ALTER TABLE ADD COLUMN) ---"
cassandra_cql "ALTER TABLE ${KEYSPACE}.${TABLE} ADD schema_demo_col text;"
sleep 3
cassandra_cql "INSERT INTO ${KEYSPACE}.${TABLE} (a,b,schema_demo_col) VALUES ('evolved','row','new-column-value');"
sleep 5

check "row 'evolved' present in Cassandra with the new column" bash -c \
  "podman exec cassandra cqlsh -e \"SELECT schema_demo_col FROM ${KEYSPACE}.${TABLE} WHERE a='evolved';\" | grep -q new-column-value"

versions_after_evolution="$(subject_version_count)"
check "a 2nd schema version registered under ${DATA_TOPIC}-value" test "${versions_after_evolution}" -ge 2

v1_fields="$(curl -sf "${SCHEMA_REGISTRY_URL}/subjects/${DATA_TOPIC}-value/versions/1" | python3 -c "import json,sys; print(len(json.loads(json.load(sys.stdin)['schema'])['fields']))")"
v2_fields="$(curl -sf "${SCHEMA_REGISTRY_URL}/subjects/${DATA_TOPIC}-value/versions/latest" | python3 -c "import json,sys; print(len(json.loads(json.load(sys.stdin)['schema'])['fields']))")"
log "Schema v1 field count=${v1_fields}, latest field count=${v2_fields} (latest should be v1 + 1 for schema_demo_col)"
check "latest schema has exactly one more field than v1" test "$((v2_fields - v1_fields))" -eq 1

log "--- Scenario 2: auto-register disabled rejects an unregistered schema ---"
reconfigure_connector "schema.registry.autoRegisterSchemas" "false"

before_offset="$(data_topic_offset)"

cassandra_cql "ALTER TABLE ${KEYSPACE}.${TABLE} ADD schema_demo_col2 text;"
sleep 3
cassandra_cql "INSERT INTO ${KEYSPACE}.${TABLE} (a,b,schema_demo_col2) VALUES ('rejected','row','should-not-publish');"
sleep 8

check "connect-worker.log shows the registry rejecting the unregistered schema" bash -c \
  "grep -qi 'schema registry\|RestClientException\|not registered\|Schema not found' '${REPO_ROOT}/connect-worker.log'"

after_offset="$(data_topic_offset)"
log "Data topic offset before=${before_offset} after=${after_offset} (should be unchanged -- the row is stuck retrying, not published)"
check "the rejected row was never published to the data topic" test "${before_offset}" -eq "${after_offset}"

log "Re-enabling auto-register so the stack is left in its normal state"
reconfigure_connector "schema.registry.autoRegisterSchemas" "true"
sleep 5
final_offset="$(data_topic_offset)"
log "Data topic offset after re-enabling auto-register: ${final_offset} (should be > ${after_offset} -- the stuck retry now succeeds and registers v3)"
check "the previously-stuck row publishes once auto-register is back on" test "${final_offset}" -gt "${after_offset}"

echo
if [ "${fail}" -eq 0 ]; then
  log "All schema registry checks passed."
else
  log "One or more schema registry checks FAILED — see above."
fi
exit "${fail}"
