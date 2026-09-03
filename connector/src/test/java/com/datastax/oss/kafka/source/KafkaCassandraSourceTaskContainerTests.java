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

import com.datastax.oss.cdc.AgentTestUtil;
import com.datastax.oss.cdc.CassandraSourceConnectorConfig;
import com.datastax.oss.cdc.ConverterAndQuery;
import com.datastax.oss.cdc.converters.AvroRowConverter;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.testcontainers.cassandra.CassandraContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.FileWriter;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs KafkaCassandraSourceTask directly (bypassing the Kafka Connect worker/REST layer)
 * against a real Kafka broker and a real Cassandra node with the actual CDC agent attached,
 * proving the end-to-end read path: agent publishes a dirty event, the task deduplicates and
 * reads the row back from Cassandra, and emits a SourceRecord with the real row content.
 */
@Slf4j
public class KafkaCassandraSourceTaskContainerTests {

    private static final String CONTAINER_KAFKA_CONFIG_PATH = "/etc/cassandra/cdc-kafka.conf";

    private static Network testNetwork;
    private static KafkaContainer kafkaContainer;
    private static CassandraContainer<?> cassandraContainer;

    @BeforeAll
    static void startContainers() throws Exception {
        testNetwork = Network.newNetwork();
        kafkaContainer = new KafkaContainer(AgentTestUtil.KAFKA_IMAGE)
                .withNetwork(testNetwork)
                .withNetworkAliases("kafka");
        kafkaContainer.start();

        File kafkaConf = File.createTempFile("cdc-kafka-agent-", ".conf");
        kafkaConf.deleteOnExit();
        try (FileWriter fw = new FileWriter(kafkaConf)) {
            fw.write("bootstrapServers=kafka:9092\n");
        }

        String agentParams = String.format(
                "platform=KAFKA,kafkaConfigFile=%s,topicPrefix=events-", CONTAINER_KAFKA_CONFIG_PATH);
        DockerImageName cassandraImage = DockerImageName.parse(
                Optional.ofNullable(System.getenv("CASSANDRA_IMAGE")).orElse("cassandra:4.0.4"))
                .asCompatibleSubstituteFor("cassandra");
        cassandraContainer = CassandraContainer.createCassandraContainerWithAgent(
                cassandraImage, testNetwork, 1,
                System.getProperty("agentBuildDir"), "agent-c4", agentParams, "c4");
        cassandraContainer.withCopyFileToContainer(
                MountableFile.forHostPath(kafkaConf.getAbsolutePath()), CONTAINER_KAFKA_CONFIG_PATH);
        cassandraContainer.start();

        try (CqlSession session = cassandraContainer.getCqlSession()) {
            session.execute("CREATE KEYSPACE IF NOT EXISTS ks1 WITH replication = "
                    + "{'class':'SimpleStrategy','replication_factor':1}");
            session.execute("CREATE TABLE IF NOT EXISTS ks1.tbl1 (a text, b text, PRIMARY KEY (a)) WITH cdc=true");
            session.execute("CREATE TABLE IF NOT EXISTS ks1.tbl_schema_evolve (a text, b text, PRIMARY KEY (a)) WITH cdc=true");
        }
    }

    @AfterAll
    static void stopContainers() {
        if (cassandraContainer != null) {
            cassandraContainer.close();
        }
        if (kafkaContainer != null) {
            kafkaContainer.close();
        }
        if (testNetwork != null) {
            testNetwork.close();
        }
    }

    @Test
    void should_read_back_row_inserted_after_cdc_event() throws Exception {
        try (CqlSession session = cassandraContainer.getCqlSession()) {
            session.execute("INSERT INTO ks1.tbl1 (a, b) VALUES ('hello', 'world')");
        }

        String eventsTopic = "events-ks1.tbl1";
        KafkaConsumer<byte[], byte[]> consumer = createInternalConsumer(eventsTopic);

        KafkaCassandraSourceTask task = new KafkaCassandraSourceTask();
        task.config = new CassandraSourceConnectorConfig(ImmutableMap.<String, String>builder()
                .put(CassandraSourceConnectorConfig.KEYSPACE_NAME_CONFIG, "ks1")
                .put(CassandraSourceConnectorConfig.TABLE_NAME_CONFIG, "tbl1")
                .put(CassandraSourceConnectorConfig.EVENTS_TOPIC_NAME_CONFIG, eventsTopic)
                .put(CassandraSourceConnectorConfig.OUTPUT_TOPIC_CONFIG, "data-ks1.tbl1")
                .put(CassandraSourceConnectorConfig.CONTACT_POINTS_OPT, cassandraContainer.getHost())
                .put(CassandraSourceConnectorConfig.PORT_OPT,
                        String.valueOf(cassandraContainer.getMappedPort(CassandraContainer.CQL_PORT)))
                .put(CassandraSourceConnectorConfig.DC_OPT, cassandraContainer.getLocalDc())
                .build());
        task.mutationCache = new com.datastax.oss.cdc.MutationCache<>(3, 1000, Duration.ofMinutes(5));
        task.eventsTopic = eventsTopic;
        task.outputTopic = "data-ks1.tbl1";
        task.consumer = consumer;
        task.queryExecutor = OrderedExecutor.newBuilder()
                .name("cdc-query-executor-it")
                .numThreads(1)
                .build();
        task.initCassandraClientWithRetry();

        try {
            List<SourceRecord> records = pollUntilNonEmpty(task, 30);

            assertThat(records).hasSize(1);
            SourceRecord record = records.get(0);
            assertThat(record.topic()).isEqualTo("data-ks1.tbl1");

            GenericRecord row = decodeAvro("tbl1", (byte[]) record.value());
            assertThat(row.get("b").toString()).isEqualTo("world");
        } finally {
            task.stop();
        }
    }

    @Test
    void should_pick_up_altered_column_and_still_decode_downstream_correctly() throws Exception {
        String table = "tbl_schema_evolve";
        String eventsTopic = "events-ks1." + table;
        String outputTopic = "data-ks1." + table;

        try (CqlSession session = cassandraContainer.getCqlSession()) {
            session.execute("INSERT INTO ks1." + table + " (a, b) VALUES ('row1', 'before')");
        }

        KafkaConsumer<byte[], byte[]> consumer = createInternalConsumer(eventsTopic);
        KafkaCassandraSourceTask task = new KafkaCassandraSourceTask();
        task.config = new CassandraSourceConnectorConfig(ImmutableMap.<String, String>builder()
                .put(CassandraSourceConnectorConfig.KEYSPACE_NAME_CONFIG, "ks1")
                .put(CassandraSourceConnectorConfig.TABLE_NAME_CONFIG, table)
                .put(CassandraSourceConnectorConfig.EVENTS_TOPIC_NAME_CONFIG, eventsTopic)
                .put(CassandraSourceConnectorConfig.OUTPUT_TOPIC_CONFIG, outputTopic)
                .put(CassandraSourceConnectorConfig.CONTACT_POINTS_OPT, cassandraContainer.getHost())
                .put(CassandraSourceConnectorConfig.PORT_OPT,
                        String.valueOf(cassandraContainer.getMappedPort(CassandraContainer.CQL_PORT)))
                .put(CassandraSourceConnectorConfig.DC_OPT, cassandraContainer.getLocalDc())
                .build());
        task.mutationCache = new com.datastax.oss.cdc.MutationCache<>(3, 1000, Duration.ofMinutes(5));
        task.eventsTopic = eventsTopic;
        task.outputTopic = outputTopic;
        task.consumer = consumer;
        task.queryExecutor = OrderedExecutor.newBuilder()
                .name("cdc-query-executor-it")
                .numThreads(1)
                .build();
        task.initCassandraClientWithRetry();

        try {
            List<SourceRecord> beforeAlter = pollUntilNonEmpty(task, 30);
            assertThat(beforeAlter).hasSize(1);
            GenericRecord rowBeforeAlter = decodeAvro(table, (byte[]) beforeAlter.get(0).value());
            assertThat(rowBeforeAlter.get("b").toString()).isEqualTo("before");

            try (CqlSession session = cassandraContainer.getCqlSession()) {
                session.execute("ALTER TABLE ks1." + table + " ADD c text");
            }
            // The CQL driver's SchemaChangeListener fires asynchronously (schema agreement across
            // the cluster) and swaps task.valueConverterAndQuery in place - wait for that swap
            // rather than assuming it's immediate.
            waitUntilValueConverterCovers(task, "c", 30);

            try (CqlSession session = cassandraContainer.getCqlSession()) {
                session.execute("INSERT INTO ks1." + table + " (a, b, c) VALUES ('row2', 'before2', 'after')");
            }
            List<SourceRecord> afterAlter = pollUntilNonEmpty(task, 30);
            assertThat(afterAlter).hasSize(1);

            // Simulates a downstream consumer: there is no schema ID embedded in the record (see
            // the schema-evolution TODO on buildSourceRecord), so a real consumer has to know out
            // of band to re-fetch the current table schema in order to decode a post-alter record.
            GenericRecord rowAfterAlter = decodeAvro(table, (byte[]) afterAlter.get(0).value());
            assertThat(rowAfterAlter.get("b").toString()).isEqualTo("before2");
            assertThat(rowAfterAlter.get("c").toString()).isEqualTo("after");
        } finally {
            task.stop();
        }
    }

    private void waitUntilValueConverterCovers(KafkaCassandraSourceTask task, String columnName, int timeoutSeconds) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutSeconds * 1000L;
        while (System.currentTimeMillis() < deadline) {
            ConverterAndQuery<?> valueConverterAndQuery = task.valueConverterAndQuery;
            if (valueConverterAndQuery != null && valueConverterAndQuery.getConverter() instanceof AvroRowConverter) {
                org.apache.avro.Schema schema = ((AvroRowConverter) valueConverterAndQuery.getConverter()).nativeSchema;
                if (schema.getField(columnName) != null) {
                    return;
                }
            }
            Thread.sleep(200);
        }
        throw new AssertionError("Timed out waiting for value converter to pick up column " + columnName);
    }

    private KafkaConsumer<byte[], byte[]> createInternalConsumer(String eventsTopic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "kafka-cassandra-source-task-it");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
        TopicPartition tp = new TopicPartition(eventsTopic, 0);
        consumer.assign(Collections.singletonList(tp));
        consumer.seekToBeginning(Collections.singletonList(tp));
        return consumer;
    }

    private List<SourceRecord> pollUntilNonEmpty(KafkaCassandraSourceTask task, int timeoutSeconds) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutSeconds * 1000L;
        while (System.currentTimeMillis() < deadline) {
            List<SourceRecord> records = task.poll();
            if (!records.isEmpty()) {
                return records;
            }
        }
        return Collections.emptyList();
    }

    private GenericRecord decodeAvro(String tableName, byte[] bytes) throws Exception {
        try (CqlSession session = cassandraContainer.getCqlSession()) {
            com.datastax.oss.driver.api.core.metadata.schema.TableMetadata table =
                    session.getMetadata().getKeyspace("ks1").get().getTable(tableName).get();
            // the value converter's schema covers only non-primary-key columns (setValueConverterAndQuery)
            List<com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata> nonPkColumns =
                    table.getColumns().values().stream()
                            .filter(c -> !table.getPrimaryKey().contains(c))
                            .collect(java.util.stream.Collectors.toList());
            org.apache.avro.Schema schema = new AvroRowConverter(
                    session.getMetadata().getKeyspace("ks1").get(), table, nonPkColumns).nativeSchema;
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            return reader.read(null, decoder);
        }
    }
}
