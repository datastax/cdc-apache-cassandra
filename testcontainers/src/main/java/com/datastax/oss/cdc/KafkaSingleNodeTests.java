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
package com.datastax.oss.cdc;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.testcontainers.cassandra.CassandraContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Base class for agent integration tests with a single Cassandra node publishing CDC mutations to
 * Kafka via the messaging abstraction (registry-less raw AVRO serialization).
 *
 * <p>The agent runs inside the Cassandra container and reaches the broker over the shared Docker
 * network at {@link #KAFKA_INTERNAL_BOOTSTRAP}; the test's own consumer connects from the host via
 * {@link KafkaContainer#getBootstrapServers()}.
 */
@Slf4j
@Tag("kafka")
public abstract class KafkaSingleNodeTests {

    /**
     * Network alias + internal (BROKER) listener port used by the in-container agent to reach Kafka.
     */
    public static final String KAFKA_NETWORK_ALIAS = "kafka";
    public static final String KAFKA_INTERNAL_BOOTSTRAP = KAFKA_NETWORK_ALIAS + ":9092";

    private static Network testNetwork;
    private static KafkaContainer kafkaContainer;

    final AgentTestUtil.Version version;

    public KafkaSingleNodeTests(AgentTestUtil.Version version) {
        this.version = version;
    }

    public abstract CassandraContainer<?> createCassandraContainer(int nodeIndex, String kafkaBootstrapServers, Network testNetwork);

    public void drain(CassandraContainer... cassandraContainers) throws IOException, InterruptedException {
        // do nothing by default
    }

    public abstract int getSegmentSize();

    @BeforeAll
    public static void initBeforeClass() throws Exception {
        testNetwork = Network.newNetwork();
        kafkaContainer = new KafkaContainer(AgentTestUtil.KAFKA_IMAGE)
                .withNetwork(testNetwork)
                .withNetworkAliases(KAFKA_NETWORK_ALIAS)
                .withKraft()
                .withStartupTimeout(Duration.ofSeconds(120));
        kafkaContainer.start();
    }

    @AfterAll
    public static void closeAfterAll() {
        if (kafkaContainer != null) {
            kafkaContainer.close();
        }
    }

    protected KafkaConsumer<byte[], byte[]> createKafkaConsumer(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return new KafkaConsumer<>(props);
    }

    /**
     * Poll a topic until {@code expected} records are received (or the timeout elapses), asserting
     * the count and that each value decodes back into a {@link MutationValue}.
     */
    private void assertMutations(String topic, int expected, String groupId) {
        try (KafkaConsumer<byte[], byte[]> consumer = createKafkaConsumer(groupId)) {
            consumer.subscribe(Collections.singletonList(topic));

            int messageCount = 0;
            long startTime = System.currentTimeMillis();
            while (messageCount < expected && (System.currentTimeMillis() - startTime) < 90000) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(5));
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    messageCount++;
                    assertNotNull(record.value(), "Message value should not be null");
                    // Validate the registry-less raw-AVRO value round-trips to a MutationValue.
                    MutationValue mutationValue = MutationValueCodec.deserialize(record.value());
                    assertNotNull(mutationValue, "Value should decode to a MutationValue");
                    assertTrue(mutationValue.getMd5Digest() != null && !mutationValue.getMd5Digest().isEmpty(),
                            "MutationValue should carry an md5Digest");
                    log.info("Received Kafka CDC message: topic={}, partition={}, offset={}, value={}",
                            record.topic(), record.partition(), record.offset(), mutationValue);
                }
            }

            assertEquals(expected, messageCount, "Unexpected number of CDC mutations on topic " + topic);
        }
    }

    @Test
    public void testBasicProducer() throws InterruptedException, IOException {
        try (CassandraContainer<?> cassandraContainer1 =
                     createCassandraContainer(1, KAFKA_INTERNAL_BOOTSTRAP, testNetwork)) {
            cassandraContainer1.start();

            try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS ks1 WITH replication = {'class':'SimpleStrategy','replication_factor':'1'};");
                cqlSession.execute("CREATE TABLE IF NOT EXISTS ks1.table1 (id text PRIMARY KEY, a int) WITH cdc=true");
                cqlSession.execute("INSERT INTO ks1.table1 (id, a) VALUES('1',1)");
                cqlSession.execute("INSERT INTO ks1.table1 (id, a) VALUES('2',2)");
                cqlSession.execute("INSERT INTO ks1.table1 (id, a) VALUES('3',3)");
            }

            drain(cassandraContainer1);

            assertMutations("events-ks1.table1", 3, "test-basic-producer");
        }
    }

    @Test
    public void testMultipleTablesProducer() throws InterruptedException, IOException {
        try (CassandraContainer<?> cassandraContainer1 =
                     createCassandraContainer(1, KAFKA_INTERNAL_BOOTSTRAP, testNetwork)) {
            cassandraContainer1.start();

            try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS ks2 WITH replication = {'class':'SimpleStrategy','replication_factor':'1'};");
                cqlSession.execute("CREATE TABLE IF NOT EXISTS ks2.table1 (id text PRIMARY KEY, a int) WITH cdc=true");
                cqlSession.execute("CREATE TABLE IF NOT EXISTS ks2.table2 (a text, b int, c int, PRIMARY KEY(a,b)) WITH cdc=true");

                cqlSession.execute("INSERT INTO ks2.table1 (id, a) VALUES('1',1)");
                cqlSession.execute("INSERT INTO ks2.table1 (id, a) VALUES('2',2)");

                cqlSession.execute("INSERT INTO ks2.table2 (a,b,c) VALUES('1',1,1)");
                cqlSession.execute("INSERT INTO ks2.table2 (a,b,c) VALUES('2',1,1)");
            }

            drain(cassandraContainer1);

            assertMutations("events-ks2.table1", 2, "test-multi-table1");
            assertMutations("events-ks2.table2", 2, "test-multi-table2");
        }
    }
}
