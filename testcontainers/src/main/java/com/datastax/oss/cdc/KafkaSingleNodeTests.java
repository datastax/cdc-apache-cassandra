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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Base class for KafkaMutationSender integration tests with a single Cassandra node.
 *
 * <p>Starts one {@link KafkaContainer} and one Cassandra container (provided by the
 * concrete subclass) with the CDC agent configured to use the Kafka platform.
 * Tests write to Cassandra and verify that CDC events arrive on the expected Kafka topic.
 */
@Slf4j
public abstract class KafkaSingleNodeTests {

    /** The Confluent Platform version embedded in the Kafka testcontainers image. */
    private static final DockerImageName KAFKA_IMAGE =
            DockerImageName.parse("confluentinc/cp-kafka:7.4.0");

    private static Network testNetwork;
    private static KafkaContainer kafkaContainer;

    public abstract CassandraContainer<?> createCassandraContainer(
            int nodeIndex, String kafkaBootstrapServers, Network testNetwork) throws IOException;

    @BeforeAll
    public static void initBeforeClass() {
        testNetwork = Network.newNetwork();
        kafkaContainer = new KafkaContainer(KAFKA_IMAGE)
                .withNetwork(testNetwork)
                .withNetworkAliases("kafka")
                .withCreateContainerCmdModifier(cmd -> cmd.withName("kafka"));
        kafkaContainer.start();
        log.info("Kafka broker started, bootstrap servers: {}", kafkaContainer.getBootstrapServers());
    }

    @AfterAll
    public static void closeAfterClass() {
        if (kafkaContainer != null) {
            kafkaContainer.close();
        }
        if (testNetwork != null) {
            testNetwork.close();
        }
    }

    /**
     * Returns the bootstrap servers string as seen from <em>inside</em> the Docker
     * network (i.e. what the Cassandra container agent should use).
     */
    protected static String internalBootstrapServers() {
        // KafkaContainer exposes the internal listener on port 9092 under its network alias
        return "kafka:9092";
    }

    /**
     * Returns the bootstrap servers string as seen from <em>outside</em> Docker
     * (i.e. what the test's KafkaConsumer should use).
     */
    protected static String externalBootstrapServers() {
        return kafkaContainer.getBootstrapServers();
    }

    /**
     * Writes a temporary Kafka config file containing {@code bootstrapServers} for use
     * as the agent's {@code kafkaConfigFile} parameter.
     */
    protected static File writeKafkaConfigFile(String bootstrapServers) throws IOException {
        File tmp = File.createTempFile("cdc-kafka-agent-", ".conf");
        tmp.deleteOnExit();
        try (FileWriter fw = new FileWriter(tmp)) {
            fw.write("bootstrapServers=" + bootstrapServers + "\n");
        }
        return tmp;
    }

    /**
     * Creates a {@link KafkaConsumer} connected to the external (host-mapped) broker,
     * subscribed to the given topic, starting from the earliest offset.
     */
    protected KafkaConsumer<byte[], byte[]> createConsumer(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, externalBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + topic + "-" + System.nanoTime());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }

    /**
     * Polls until {@code expectedCount} records arrive or the timeout is exceeded.
     * Returns {@code null} if no record arrives in time.
     */
    protected ConsumerRecord<byte[], byte[]> pollOne(KafkaConsumer<byte[], byte[]> consumer,
                                                      int timeoutSeconds) {
        long deadline = System.currentTimeMillis() + timeoutSeconds * 1000L;
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<byte[], byte[]> records =
                    consumer.poll(Duration.ofSeconds(2));
            if (!records.isEmpty()) {
                return records.iterator().next();
            }
        }
        return null;
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    /**
     * Verifies that a basic INSERT produces a CDC event on the events topic
     * with a non-empty Avro key that round-trips correctly.
     */
    @Test
    public void testSchema() throws Exception {
        File kafkaConf = writeKafkaConfigFile(internalBootstrapServers());
        try (CassandraContainer<?> cassandra =
                     createCassandraContainer(1, kafkaConf.getAbsolutePath(), testNetwork)) {
            cassandra.start();

            try (CqlSession session = cassandra.getCqlSession()) {
                session.execute("CREATE KEYSPACE IF NOT EXISTS ks1 WITH replication = " +
                        "{'class':'SimpleStrategy','replication_factor':'1'};");
                session.execute("CREATE TABLE IF NOT EXISTS ks1.tbl1 " +
                        "(id text, val text, PRIMARY KEY (id)) WITH cdc=true;");
                session.execute("INSERT INTO ks1.tbl1 (id, val) VALUES ('hello', 'world');");
            }

            try (KafkaConsumer<byte[], byte[]> consumer = createConsumer("events-ks1.tbl1")) {
                ConsumerRecord<byte[], byte[]> rec = pollOne(consumer, 90);
                assertNotNull(rec, "No CDC event received — check agent log");

                // key must be a valid Avro-binary record
                assertTrue(rec.key() != null && rec.key().length > 0,
                        "Avro key bytes must be non-empty");
                assertTrue(rec.value() != null && rec.value().length > 0,
                        "Avro value bytes must be non-empty");

                // Verify the key can be decoded as Avro (schema has at least one field "id")
                Schema keySchema = buildKeySchema();
                GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(keySchema);
                BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(rec.key(), null);
                GenericRecord keyRecord = reader.read(null, decoder);
                assertEquals("hello", keyRecord.get("id").toString());

                // segpos header must be present
                assertNotNull(rec.headers().lastHeader(Constants.SEGMENT_AND_POSITION),
                        "segpos header must be present");
            }
        }
    }

    /**
     * Verifies that multiple inserts each produce an event on the topic.
     */
    @Test
    public void testMultipleMutations() throws Exception {
        int numMutations = 10;
        File kafkaConf = writeKafkaConfigFile(internalBootstrapServers());
        try (CassandraContainer<?> cassandra =
                     createCassandraContainer(1, kafkaConf.getAbsolutePath(), testNetwork)) {
            cassandra.start();

            try (CqlSession session = cassandra.getCqlSession()) {
                session.execute("CREATE KEYSPACE IF NOT EXISTS mt1 WITH replication = " +
                        "{'class':'SimpleStrategy','replication_factor':'1'};");
                session.execute("CREATE TABLE IF NOT EXISTS mt1.tbl1 " +
                        "(id int, val text, PRIMARY KEY (id)) WITH cdc=true;");
                for (int i = 0; i < numMutations; i++) {
                    session.execute("INSERT INTO mt1.tbl1 (id, val) VALUES (?, ?);",
                            i, "value-" + i);
                }
            }

            int received = 0;
            long deadline = System.currentTimeMillis() + 120_000L;
            try (KafkaConsumer<byte[], byte[]> consumer = createConsumer("events-mt1.tbl1")) {
                while (received < numMutations && System.currentTimeMillis() < deadline) {
                    ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(2));
                    for (ConsumerRecord<byte[], byte[]> rec : records) {
                        assertNotNull(rec.headers().lastHeader(Constants.SEGMENT_AND_POSITION),
                                "segpos header must be present");
                        assertNotNull(rec.key());
                        assertNotNull(rec.value());
                        received++;
                    }
                }
            }
            assertEquals(numMutations, received,
                    "Expected " + numMutations + " CDC events but received " + received);
        }
    }

    /**
     * Builds a minimal Avro schema for the `ks1.tbl1` key (single string PK field `id`).
     * Concrete subclasses can override if their schema differs.
     */
    protected Schema buildKeySchema() {
        Schema nullableString = Schema.createUnion(
                Schema.create(Schema.Type.NULL),
                Schema.create(Schema.Type.STRING));
        return Schema.createRecord("ks1_tbl1", null, "com.datastax.oss.cdc", false,
                Collections.singletonList(
                        new Schema.Field("id", nullableString, null, (Object) null)));
    }

    // -------------------------------------------------------------------------
    // Helper
    // -------------------------------------------------------------------------

    protected static String headerAsString(ConsumerRecord<byte[], byte[]> rec, String key) {
        org.apache.kafka.common.header.Header h = rec.headers().lastHeader(key);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }
}
