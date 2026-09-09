#!/usr/bin/env bash
# Runs the connector's fast, mock-based Kafka-platform unit tests (no containers, no
# broker, no live Cassandra needed) -- covers config validation, the Avro/JSON converters,
# connector/task config, and KafkaCassandraSourceTask's poll/retry/close behavior,
# including the queryExecutor shutdown-ordering fix in close().
#
# Deliberately excludes KafkaCassandraSourceTaskContainerTests: it spins up a real
# Testcontainers KafkaContainer + CassandraContainer, which needs a Docker daemon --
# this environment only has podman (see DEMO-PODMAN.md), so that class hangs here rather
# than failing outright (Testcontainers waits on the (absent) Docker socket). Run it
# separately on a machine with Docker if you need that coverage.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
cd "${REPO_ROOT}"

KAFKA_UNIT_TEST_CLASSES=(
  com.datastax.oss.cdc.CassandraSourceConnectorConfigKafkaTest
  com.datastax.oss.kafka.source.converters.KafkaAvroConverterKafkaTest
  com.datastax.oss.kafka.source.converters.KafkaAvroConverterSchemaRegistryKafkaTest
  com.datastax.oss.kafka.source.converters.KafkaJsonConverterKafkaTest
  com.datastax.oss.kafka.source.KafkaCassandraSourceConnectorKafkaTest
  com.datastax.oss.kafka.source.KafkaCassandraSourceTaskKafkaTest
  com.datastax.oss.kafka.source.KafkaCassandraSourceTaskRetryTest
)

test_args=()
for class in "${KAFKA_UNIT_TEST_CLASSES[@]}"; do
  test_args+=(--tests "${class}")
done

log "Running ${#KAFKA_UNIT_TEST_CLASSES[@]} Kafka-platform unit test classes (container-based tests excluded)"
./gradlew :connector:test -PbrokerType=kafka "${test_args[@]}"
