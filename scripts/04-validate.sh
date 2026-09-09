#!/usr/bin/env bash
# Step 4: validate the data actually made it through the pipeline — Cassandra state,
# connector/task health, and the Kafka output topic's contents. Exits non-zero if any
# check fails, so this can gate a CI job or a "is the demo healthy" script.
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

check "connector ${CONNECTOR_NAME} is RUNNING" bash -c \
  "curl -sf http://localhost:8083/connectors/${CONNECTOR_NAME}/status | grep -q '\"state\":\"RUNNING\"'"

check "task 0 is RUNNING (not the connector's own state)" bash -c \
  "curl -sf http://localhost:8083/connectors/${CONNECTOR_NAME}/status | python3 -c \"
import json, sys
d = json.load(sys.stdin)
sys.exit(0 if d['tasks'] and d['tasks'][0]['state'] == 'RUNNING' else 1)
\""

check "row 'foo' present in Cassandra with the post-update value" bash -c \
  "podman exec cassandra cqlsh -e \"SELECT b FROM ${KEYSPACE}.${TABLE} WHERE a='foo';\" | grep -q bar-updated"

check "row 'hello' present in Cassandra" bash -c \
  "podman exec cassandra cqlsh -e \"SELECT b FROM ${KEYSPACE}.${TABLE} WHERE a='hello';\" | grep -q world"

check "row 'tmp' absent from Cassandra (deleted)" bash -c \
  "! podman exec cassandra cqlsh -e \"SELECT a FROM ${KEYSPACE}.${TABLE} WHERE a='tmp';\" | grep -q '^ tmp'"

check "events topic has received mutations" bash -c \
  "[ \"\$(podman exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic ${EVENTS_TOPIC} | cut -d: -f3)\" -gt 0 ]"

check "data topic has records (dedup + read-back produced output)" bash -c \
  "[ \"\$(podman exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic ${DATA_TOPIC} | cut -d: -f3)\" -gt 0 ]"

check "data topic reflects the coalesced update (current state, not stale mutation)" bash -c \
  "podman exec kafka /opt/kafka/bin/kafka-dump-log.sh --deep-iteration --print-data-log \
     --files /tmp/kafka-logs/${DATA_TOPIC}-0/00000000000000000000.log 2>/dev/null | grep -qa bar-updated"

check "data topic has a tombstone (null payload) for the deleted key" bash -c \
  "podman exec kafka /opt/kafka/bin/kafka-dump-log.sh --deep-iteration --print-data-log \
     --files /tmp/kafka-logs/${DATA_TOPIC}-0/00000000000000000000.log 2>/dev/null | grep -Eqa 'valueSize: -1.*key: .tmp'"

check "schema registered under ${DATA_TOPIC}-value" bash -c \
  "curl -sf '${SCHEMA_REGISTRY_URL}/subjects/${DATA_TOPIC}-value/versions' | grep -q '\[1\]'"

check "data topic values are in Confluent wire format (magic byte 0x0)" bash -c \
  "podman exec kafka /opt/kafka/bin/kafka-dump-log.sh --deep-iteration --print-data-log \
     --files /tmp/kafka-logs/${DATA_TOPIC}-0/00000000000000000000.log 2>/dev/null | grep -q 'payload: .\\\\x00'"

echo
if [ "${fail}" -eq 0 ]; then
  log "All validations passed. Data written to Cassandra propagated end-to-end through the CDC agent -> Kafka -> KafkaCassandraSourceConnector pipeline."
else
  log "One or more validations FAILED — see above. Check connect-worker.log and 'podman logs cassandra'/'podman logs kafka'."
fi
exit "${fail}"
