/**
 * Copyright DataStax, Inc 2021.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.kafka.source;

import com.datastax.oss.cdc.CassandraSourceConnectorConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Covers {@link KafkaCassandraSourceTask#start} and {@link KafkaCassandraSourceTask#stop} without a
 * broker or Cassandra: the internal consumer is created but never polled, and the Cassandra
 * client initialisation is stubbed out.
 */
class KafkaCassandraSourceTaskLifecycleKafkaTest {

    private static final String EVENTS_TOPIC = "events-ks1.table1";

    /** Skips the Cassandra connection, which start() would otherwise retry until it succeeds. */
    static class OfflineTask extends KafkaCassandraSourceTask {
        boolean cassandraInitialised;

        @Override
        void initCassandraClientWithRetry() {
            cassandraInitialised = true;
        }
    }

    private final OfflineTask task = new OfflineTask();

    private static Map<String, String> baseProps() {
        Map<String, String> props = new HashMap<>();
        props.put(CassandraSourceConnectorConfig.KEYSPACE_NAME_CONFIG, "ks1");
        props.put(CassandraSourceConnectorConfig.TABLE_NAME_CONFIG, "table1");
        props.put(CassandraSourceConnectorConfig.EVENTS_TOPIC_NAME_CONFIG, EVENTS_TOPIC);
        props.put(CassandraSourceConnectorConfig.OUTPUT_TOPIC_CONFIG, "data-ks1.table1");
        props.put(CassandraSourceConnectorConfig.INTERNAL_CONSUMER_BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(KafkaCassandraSourceConnector.INTERNAL_CONSUMER_PARTITIONS_CONFIG, "0, 1");
        return props;
    }

    /** Partition 0 has a committed offset, partition 1 has none. */
    private void initializeContext() {
        OffsetStorageReader offsetReader = mock(OffsetStorageReader.class);
        when(offsetReader.offset(any())).thenReturn(null);
        when(offsetReader.offset(argThat(partition -> partition != null && Integer.valueOf(0).equals(partition.get("partition")))))
                .thenReturn(Collections.singletonMap("offset", 5L));
        SourceTaskContext context = mock(SourceTaskContext.class);
        when(context.offsetStorageReader()).thenReturn(offsetReader);
        task.initialize(context);
    }

    @AfterEach
    void tearDown() {
        task.stop();
    }

    @Test
    void startsWithDefaults() {
        initializeContext();

        task.start(baseProps());

        assertThat(task.version()).isNotNull();
        assertThat(task.cassandraInitialised).isTrue();
        assertThat(task.heartbeatTopic).isEqualTo("data-ks1.table1-heartbeat");
        assertThat(task.columnPattern).isEmpty();
        assertThat(task.assignedPartitions).containsExactly(
                new TopicPartition(EVENTS_TOPIC, 0), new TopicPartition(EVENTS_TOPIC, 1));
        assertThat(task.consumer.assignment()).isEqualTo(new HashSet<>(task.assignedPartitions));
        assertThat(task.mutationCache).isNotNull();
        assertThat(task.queryExecutor).isNotNull();
        assertThat(task.schemaRegistrySerializer).isNull();
    }

    @Test
    void startsWithSchemaRegistryColumnsFilterAndRateLimit() {
        initializeContext();
        Map<String, String> props = baseProps();
        props.put(CassandraSourceConnectorConfig.HEARTBEAT_TOPIC_CONFIG, "custom-heartbeat");
        props.put(CassandraSourceConnectorConfig.COLUMNS_REGEXP_CONFIG, "a|b");
        props.put(CassandraSourceConnectorConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://registry");
        props.put(CassandraSourceConnectorConfig.QUERY_RATE_LIMIT_CONFIG, "100");

        task.start(props);

        assertThat(task.heartbeatTopic).isEqualTo("custom-heartbeat");
        assertThat(task.columnPattern).isPresent();
        assertThat(task.schemaRegistrySerializer).isNotNull();
        assertThat(task.keySchemaRegistrySerializer).isNotNull();

        task.stop();
        assertThat(task.schemaRegistrySerializer).isNull();
        assertThat(task.keySchemaRegistrySerializer).isNull();
    }

    @Test
    void rejectsSchemaRegistryWithJsonOutput() {
        initializeContext();
        Map<String, String> props = baseProps();
        props.put(CassandraSourceConnectorConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://registry");
        props.put(CassandraSourceConnectorConfig.OUTPUT_FORMAT, "key-value-json");

        assertThatThrownBy(() -> task.start(props))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("schema.registry.url");
    }

    @Test
    void rejectsMissingPartitionAssignment() {
        initializeContext();
        Map<String, String> props = baseProps();
        props.remove(KafkaCassandraSourceConnector.INTERNAL_CONSUMER_PARTITIONS_CONFIG);

        assertThatThrownBy(() -> task.start(props))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("internal.consumer.partitions");
    }

    @Test
    void stopWithoutStartIsSafe() {
        task.stop();
        assertThat(Arrays.asList(task.consumer, task.cassandraClient, task.queryExecutor)).containsOnlyNulls();
    }
}
