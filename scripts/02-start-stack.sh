#!/usr/bin/env bash
# Step 2: start Kafka, Confluent Schema Registry, Cassandra (with the CDC agent in Kafka
# mode), the Kafka Connect worker running KafkaCassandraSourceConnector, and Confluent
# Control Center for visualization. Idempotent — safe to re-run if some pieces are already up.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
cd "${REPO_ROOT}"

podman network exists "${NETWORK}" || podman network create "${NETWORK}"

if ! podman container exists kafka; then
  log "Starting Kafka broker (KRaft, single node)"
  podman run -d --name kafka --network "${NETWORK}" -p 9092:9092 \
    -e KAFKA_NODE_ID=1 \
    -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092 \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:9093 \
    -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
    -e CLUSTER_ID=ciWo7IWazngRchmPES6q5A== \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
    apache/kafka:3.9.0
else
  log "Kafka broker already running"
fi
wait_for "Kafka broker" kafka_exec kafka-topics.sh --bootstrap-server localhost:9092 --list

cat > "${REPO_ROOT}/cdc-kafka.conf" <<EOF
bootstrapServers=kafka:9092
EOF

if ! podman container exists schema-registry; then
  log "Starting Confluent Schema Registry"
  podman run -d --name schema-registry --network "${NETWORK}" -p 8081:8081 \
    -e SCHEMA_REGISTRY_HOST_NAME=schema-registry \
    -e SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=PLAINTEXT://kafka:9092 \
    -e SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081 \
    "confluentinc/cp-schema-registry:${CONFLUENT_VERSION}"
else
  log "Schema Registry already running"
fi
wait_for "Schema Registry" curl -sf "${SCHEMA_REGISTRY_URL}/subjects"

if ! podman container exists cassandra; then
  log "Starting Cassandra with the CDC agent (platform=KAFKA)"
  podman run -d --name cassandra --network "${NETWORK}" -p 9042:9042 \
    -e MAX_HEAP_SIZE=1200m -e HEAP_NEWSIZE=300m -e DS_LICENSE=accept \
    -e CASSANDRA_DC=datacenter1 -e DC=datacenter1 \
    -v "${REPO_ROOT}/cdc-kafka.conf:/etc/cassandra/cdc-kafka.conf" \
    -e JVM_EXTRA_OPTS="-javaagent:/agent-c4-${PROJECT_VERSION}-all.jar=platform=KAFKA,kafkaConfigFile=/etc/cassandra/cdc-kafka.conf,topicPrefix=events-" \
    "${CASSANDRA_IMAGE}"
else
  log "Cassandra already running"
fi
wait_for "Cassandra" cassandra_cql "SELECT now() FROM system.local;"

cassandra_cql "CREATE KEYSPACE IF NOT EXISTS ${KEYSPACE} WITH replication = {'class':'SimpleStrategy','replication_factor':'1'};"
cassandra_cql "CREATE TABLE IF NOT EXISTS ${KEYSPACE}.${TABLE} (a text, b text, PRIMARY KEY (a)) WITH cdc=true;"

# The events topic is created lazily on the first mutation. If Connect starts before it
# exists, the connector silently comes up with zero tasks — so write one row first.
cassandra_cql "INSERT INTO ${KEYSPACE}.${TABLE} (a,b) VALUES ('__bootstrap__','init');"
wait_for "events topic" bash -c "podman exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list | grep -q ${EVENTS_TOPIC}"

if [ ! -x "${KAFKA_DIST}/bin/connect-standalone.sh" ]; then
  log "Downloading Kafka ${KAFKA_VERSION} distribution (provides connect-standalone.sh)"
  curl -sfSL -o "${REPO_ROOT}/kafka_2.13-${KAFKA_VERSION}.tgz" \
    "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_2.13-${KAFKA_VERSION}.tgz"
  tar xzf "${REPO_ROOT}/kafka_2.13-${KAFKA_VERSION}.tgz" -C "${REPO_ROOT}"
fi

log "Staging the Kafka Connect plugin"
# The plain connector jar has no bundled deps; the .nar does (under
# META-INF/bundled-dependencies/) — reuse that instead of hand-resolving the classpath.
rm -rf "${KAFKA_DIST}/plugins/cassandra-source"
mkdir -p "${KAFKA_DIST}/plugins/cassandra-source"
cp "${REPO_ROOT}/connector/build/libs/pulsar-cassandra-source-${PROJECT_VERSION}.jar" \
  "${KAFKA_DIST}/plugins/cassandra-source/"
tmpdir="$(mktemp -d)"
unzip -oq "${REPO_ROOT}/connector/build/libs/pulsar-cassandra-source-${PROJECT_VERSION}.nar" \
  "META-INF/bundled-dependencies/*.jar" -d "${tmpdir}"
mv "${tmpdir}"/META-INF/bundled-dependencies/*.jar "${KAFKA_DIST}/plugins/cassandra-source/"
rm -rf "${tmpdir}"

cat > "${REPO_ROOT}/connect-worker.properties" <<EOF
bootstrap.servers=localhost:9092
key.converter=org.apache.kafka.connect.converters.ByteArrayConverter
value.converter=org.apache.kafka.connect.converters.ByteArrayConverter
offset.storage.file.filename=${REPO_ROOT}/connect.offsets
offset.flush.interval.ms=5000
plugin.path=${KAFKA_DIST}/plugins
rest.port=8083
EOF

cat > "${REPO_ROOT}/cassandra-source-connector.properties" <<EOF
name=${CONNECTOR_NAME}
connector.class=com.datastax.oss.kafka.source.KafkaCassandraSourceConnector
tasks.max=1
keyspace=${KEYSPACE}
table=${TABLE}
events.topic=${EVENTS_TOPIC}
output.topic=${DATA_TOPIC}
internal.consumer.bootstrapServers=localhost:9092
contactPoints=localhost
loadBalancing.localDc=datacenter1
schema.registry.url=${SCHEMA_REGISTRY_URL}
EOF

if [ -f "${REPO_ROOT}/connect-worker.pid" ] && kill -0 "$(cat "${REPO_ROOT}/connect-worker.pid")" 2>/dev/null; then
  log "Connect worker already running (pid $(cat "${REPO_ROOT}/connect-worker.pid"))"
else
  log "Starting the standalone Kafka Connect worker in the background"
  nohup "${KAFKA_DIST}/bin/connect-standalone.sh" \
    "${REPO_ROOT}/connect-worker.properties" "${REPO_ROOT}/cassandra-source-connector.properties" \
    > "${REPO_ROOT}/connect-worker.log" 2>&1 &
  echo $! > "${REPO_ROOT}/connect-worker.pid"
fi

wait_for "Connect worker REST API" curl -sf http://localhost:8083/connectors
wait_for "Connector RUNNING" bash -c \
  "curl -sf http://localhost:8083/connectors/${CONNECTOR_NAME}/status | grep -q '\"state\":\"RUNNING\"'"

if ! podman container exists control-center; then
  log "Starting Confluent Control Center for visualization (http://localhost:9021)"
  # Single-broker demo cluster: internal topics must run at replication factor 1.
  # CONTROL_CENTER_CONNECT_CONNECT-DEFAULT_CLUSTER points at the host-run Connect
  # worker via podman's host gateway DNS name so Control Center's Connect tab can
  # show the connector/task status alongside the topics.
  podman run -d --name control-center --network "${NETWORK}" -p 9021:9021 \
    -e CONTROL_CENTER_BOOTSTRAP_SERVERS=kafka:9092 \
    -e CONTROL_CENTER_REPLICATION_FACTOR=1 \
    -e CONTROL_CENTER_INTERNAL_TOPICS_PARTITIONS=1 \
    -e CONTROL_CENTER_MONITORING_INTERCEPTOR_TOPIC_PARTITIONS=1 \
    -e CONFLUENT_METRICS_TOPIC_REPLICATION=1 \
    -e CONTROL_CENTER_COMMAND_TOPIC_REPLICATION=1 \
    -e CONTROL_CENTER_CONNECT_CONNECT-DEFAULT_CLUSTER=http://host.containers.internal:8083 \
    -e CONTROL_CENTER_SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_CONTAINER_URL}" \
    -e PORT=9021 \
    "confluentinc/cp-enterprise-control-center:${CONFLUENT_VERSION}"
else
  log "Control Center already running"
fi

log "Stack is up:"
log "  Cassandra:       localhost:9042"
log "  Kafka broker:    localhost:9092"
log "  Schema Registry: ${SCHEMA_REGISTRY_URL}"
log "  Connect REST:    http://localhost:8083/connectors/${CONNECTOR_NAME}/status"
log "  Control Center:  http://localhost:9021  (may take ~1-2 min to finish starting up)"
