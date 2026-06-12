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
package com.datastax.oss.kafka.sink;

import com.datastax.oss.cdc.AgentTestUtil;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.testcontainers.cassandra.CassandraContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end test of the Kafka source pipeline:
 * Cassandra (CDC agent → Kafka) → events topic → {@link CassandraSinkTask} (queries Cassandra) →
 * data topic.
 */
@Slf4j
@Tag("kafka")
public class CassandraKafkaSinkE2ETest {

    static final String KAFKA_INTERNAL_BOOTSTRAP = "kafka:9092";

    static final DockerImageName CASSANDRA_IMAGE = DockerImageName.parse(
            Optional.ofNullable(System.getenv("CASSANDRA_IMAGE"))
                    .orElse("cassandra:" + System.getProperty("cassandraVersion"))
    ).asCompatibleSubstituteFor("cassandra");

    static Network network;
    static KafkaContainer kafkaContainer;

    @BeforeAll
    static void beforeAll() {
        network = Network.newNetwork();
        kafkaContainer = new KafkaContainer(AgentTestUtil.KAFKA_IMAGE)
                .withNetwork(network)
                .withNetworkAliases("kafka")
                .withKraft()
                .withStartupTimeout(Duration.ofSeconds(120));
        kafkaContainer.start();
    }

    @AfterAll
    static void afterAll() {
        if (kafkaContainer != null) {
            kafkaContainer.close();
        }
    }

    private CassandraContainer<?> startCassandra() {
        CassandraContainer<?> cassandra = CassandraContainer.createCassandraContainerWithAgentKafka(
                CASSANDRA_IMAGE, network, 1, "c4", KAFKA_INTERNAL_BOOTSTRAP);
        cassandra.start();
        return cassandra;
    }

    private KafkaConsumer<byte[], byte[]> consumer(String group) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return new KafkaConsumer<>(props);
    }

    /** Poll a topic until {@code expected} records are collected (or timeout). */
    private List<ConsumerRecord<byte[], byte[]>> poll(String topic, int expected, String group) {
        List<ConsumerRecord<byte[], byte[]>> out = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> consumer = consumer(group)) {
            consumer.subscribe(Collections.singletonList(topic));
            long start = System.currentTimeMillis();
            while (out.size() < expected && (System.currentTimeMillis() - start) < 90000) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(5));
                records.forEach(out::add);
            }
        }
        return out;
    }

    private Map<String, String> sinkConfig(CassandraContainer<?> cassandra, String keyspace, String table,
                                           String outputFormat) {
        Map<String, String> props = new HashMap<>();
        props.put("name", "cassandra-kafka-sink-" + keyspace + "-" + table);
        props.put("contactPoints", cassandra.getHost());
        props.put("port", String.valueOf(cassandra.getMappedPort(CassandraContainer.CQL_PORT)));
        props.put("loadBalancing.localDc", cassandra.getLocalDc());
        props.put("keyspace", keyspace);
        props.put("table", table);
        props.put("outputFormat", outputFormat);
        props.put(CassandraSinkConfig.KAFKA_BOOTSTRAP_SERVERS, kafkaContainer.getBootstrapServers());
        return props;
    }

    private void runSink(Map<String, String> config, List<ConsumerRecord<byte[], byte[]>> events) {
        CassandraSinkTask task = new CassandraSinkTask();
        task.start(config);
        try {
            List<SinkRecord> sinkRecords = new ArrayList<>(events.size());
            for (ConsumerRecord<byte[], byte[]> e : events) {
                sinkRecords.add(new SinkRecord(e.topic(), e.partition(), null, e.key(), null, e.value(), e.offset()));
            }
            task.put(sinkRecords);
            task.flush(null);
        } finally {
            task.stop();
        }
    }

    @Test
    public void testAvroPipeline() {
        try (CassandraContainer<?> cassandra = startCassandra()) {
            try (CqlSession session = cassandra.getCqlSession()) {
                session.execute("CREATE KEYSPACE IF NOT EXISTS ks1 WITH replication = {'class':'SimpleStrategy','replication_factor':'1'};");
                session.execute("CREATE TABLE IF NOT EXISTS ks1.table1 (id text PRIMARY KEY, a int) WITH cdc=true");
                session.execute("INSERT INTO ks1.table1 (id, a) VALUES('1',1)");
                session.execute("INSERT INTO ks1.table1 (id, a) VALUES('2',2)");
                session.execute("INSERT INTO ks1.table1 (id, a) VALUES('3',3)");
            }

            List<ConsumerRecord<byte[], byte[]>> events = poll("events-ks1.table1", 3, "avro-events");
            assertEquals(3, events.size(), "agent should publish 3 events");

            runSink(sinkConfig(cassandra, "ks1", "table1", "key-value-avro"), events);

            List<ConsumerRecord<byte[], byte[]>> data = poll("data-ks1.table1", 3, "avro-data");
            assertEquals(3, data.size(), "connector should publish 3 rows to the data topic");
            for (ConsumerRecord<byte[], byte[]> r : data) {
                assertNotNull(r.key(), "data record key (primary key) should not be null");
                assertNotNull(r.value(), "data record value (row) should not be null for an insert");
            }
        }
    }

    @Test
    public void testJsonPipeline() {
        try (CassandraContainer<?> cassandra = startCassandra()) {
            try (CqlSession session = cassandra.getCqlSession()) {
                session.execute("CREATE KEYSPACE IF NOT EXISTS ks2 WITH replication = {'class':'SimpleStrategy','replication_factor':'1'};");
                session.execute("CREATE TABLE IF NOT EXISTS ks2.users (id text PRIMARY KEY, age int) WITH cdc=true");
                session.execute("INSERT INTO ks2.users (id, age) VALUES('alice',30)");
                session.execute("INSERT INTO ks2.users (id, age) VALUES('bob',40)");
            }

            List<ConsumerRecord<byte[], byte[]>> events = poll("events-ks2.users", 2, "json-events");
            assertEquals(2, events.size());

            runSink(sinkConfig(cassandra, "ks2", "users", "key-value-json"), events);

            List<ConsumerRecord<byte[], byte[]>> data = poll("data-ks2.users", 2, "json-data");
            assertEquals(2, data.size());
            boolean sawAge = false;
            for (ConsumerRecord<byte[], byte[]> r : data) {
                assertNotNull(r.value());
                String json = new String(r.value(), StandardCharsets.UTF_8);
                log.info("data-ks2.users JSON value: {}", json);
                assertTrue(json.contains("age"), "JSON row should contain the 'age' column: " + json);
                if (json.contains("30") || json.contains("40")) {
                    sawAge = true;
                }
            }
            assertTrue(sawAge, "expected at least one row with the inserted age value");
        }
    }
}
