#!/usr/bin/env bash
# Shared config + helpers for the scripts/ demo pipeline. Sourced, not executed directly.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_VERSION="$(grep '^version=' "${REPO_ROOT}/gradle.properties" | cut -d= -f2)"
CONFLUENT_VERSION="$(grep '^confluentVersion=' "${REPO_ROOT}/gradle.properties" | cut -d= -f2)"
KAFKA_VERSION=3.9.2

NETWORK=cdc-net
KEYSPACE=ks1
TABLE=table1
EVENTS_TOPIC="events-${KEYSPACE}.${TABLE}"
DATA_TOPIC="data-${KEYSPACE}.${TABLE}"
CONNECTOR_NAME="cassandra-source-${KEYSPACE}-${TABLE}"
KAFKA_DIST="${REPO_ROOT}/kafka_2.13-${KAFKA_VERSION}"
CASSANDRA_IMAGE="localhost/myrepo/cassandra:4.0.4-cdc"

# Confluent Schema Registry — used by the schema.registry.* connector settings to publish the
# data topic's Avro value in Confluent wire format and version it across ALTER TABLEs. The
# Connect worker runs on the host (not in a container), so it reaches the registry via the
# published host port; other containers (Control Center) reach it via the container hostname.
SCHEMA_REGISTRY_URL="http://localhost:8081"
SCHEMA_REGISTRY_CONTAINER_URL="http://schema-registry:8081"

log() { echo "[$(date +%H:%M:%S)] $*"; }

# wait_for "description" cmd arg1 arg2 ... — retries a command every 3s for up to 3 minutes.
wait_for() {
  local desc="$1"; shift
  local tries=0
  until "$@" >/dev/null 2>&1; do
    tries=$((tries + 1))
    if [ "$tries" -gt 60 ]; then
      echo "Timed out waiting for: ${desc}" >&2
      return 1
    fi
    sleep 3
  done
  log "${desc} — ready"
}

kafka_exec() { podman exec kafka /opt/kafka/bin/"$@"; }
cassandra_cql() { podman exec cassandra cqlsh -e "$1"; }
