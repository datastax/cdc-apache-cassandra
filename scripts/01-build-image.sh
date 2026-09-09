#!/usr/bin/env bash
# Step 1: build the agent + connector artifacts from source, then build the
# CDC-enabled Cassandra image with podman (no docker CLI required).
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
cd "${REPO_ROOT}"

log "Building agent + connector artifacts from source (version ${PROJECT_VERSION})"
./gradlew :agent-c4:shadowJar :agent-c4:dockerPrepare :connector:jar :connector:nar -x test

log "Building CDC-enabled Cassandra image: ${CASSANDRA_IMAGE}"
podman build \
  --build-arg CASSANDRA_VERSION=4.0.4 \
  --build-arg BUILD_VERSION="${PROJECT_VERSION}" \
  --build-arg COMMITMOG_SYNC_PERIOD_IN_MS=2000 \
  --build-arg CDC_TOTAL_SPACE_IN_MB=70 \
  -t myrepo/cassandra:4.0.4-cdc \
  agent-c4/build/docker

log "Image ready: ${CASSANDRA_IMAGE}"
podman images myrepo/cassandra
