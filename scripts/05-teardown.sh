#!/usr/bin/env bash
# Tears down everything 02-start-stack.sh created: the Connect worker process, the
# podman containers/network, and the generated config/state files at the repo root.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

log "Stopping Kafka Connect worker"
if [ -f "${REPO_ROOT}/connect-worker.pid" ]; then
  kill "$(cat "${REPO_ROOT}/connect-worker.pid")" 2>/dev/null || true
  rm -f "${REPO_ROOT}/connect-worker.pid"
fi

log "Removing containers + network"
podman rm -f cassandra kafka control-center schema-registry 2>/dev/null || true
podman network rm "${NETWORK}" 2>/dev/null || true

log "Removing generated config/state files"
rm -f "${REPO_ROOT}/connect.offsets" "${REPO_ROOT}/connect-worker.log" \
      "${REPO_ROOT}/cdc-kafka.conf" "${REPO_ROOT}/connect-worker.properties" \
      "${REPO_ROOT}/cassandra-source-connector.properties"

log "Teardown complete."
