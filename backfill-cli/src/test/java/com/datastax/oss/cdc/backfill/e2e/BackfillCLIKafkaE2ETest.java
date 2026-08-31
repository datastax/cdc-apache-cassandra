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
package com.datastax.oss.cdc.backfill.e2e;

import com.datastax.oss.cdc.AgentTestUtil;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.kafka.sink.CassandraSinkConfig;
import com.datastax.oss.kafka.sink.CassandraSinkTask;
import com.datastax.testcontainers.cassandra.CassandraContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * End-to-end test of the backfill CLI against Kafka:
 * <p>
 * Cassandra table (cdc=false) → backfill CLI ({@code --messaging-provider=kafka}, run as the
 * standalone shadow JAR) → {@code events-<ks>.<table>} topic → {@link CassandraSinkTask} (queries
 * Cassandra, in-process) → {@code data-<ks>.<table>} topic.
 * <p>
 * This is the Kafka counterpart of {@code BackfillCLIE2ETests} (which validates the Pulsar path).
 * The backfill JAR is exercised exactly as a user would run it for Kafka, and the Kafka sink is run
 * in-process — mirroring {@code CassandraKafkaSinkE2ETest} — to avoid needing a live Kafka Connect
 * runtime.
 */
@Slf4j
@Tag("kafka")
public class BackfillCLIKafkaE2ETest {

    static final DockerImageName CASSANDRA_IMAGE = DockerImageName.parse(
            Optional.ofNullable(System.getenv("CASSANDRA_IMAGE"))
                    .orElse("cassandra:" + System.getProperty("cassandraVersion"))
    ).asCompatibleSubstituteFor("cassandra");

    /** Cassandra family ("c3"/"c4"/"dse4"); selects the config-override resource directory. */
    static final String CASSANDRA_FAMILY = Optional.ofNullable(System.getProperty("cassandraFamily")).orElse("c4");

    static Network network;
    static KafkaContainer kafkaContainer;

    private Path dataDir;
    private Path logsDir;

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

    @BeforeEach
    void initDirs() throws Exception {
        dataDir = Files.createTempDirectory("data");
        logsDir = Files.createTempDirectory("logs");
    }

    @AfterEach
    void cleanupDirs() throws Exception {
        deleteRecursively(dataDir);
        deleteRecursively(logsDir);
    }

    private static void deleteRecursively(Path path) throws Exception {
        if (path == null || !Files.exists(path)) {
            return;
        }
        Files.walk(path)
                .sorted((a, b) -> b.compareTo(a))
                .forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (Exception e) {
                        log.warn("Failed to delete {}", p, e);
                    }
                });
    }

    private CassandraContainer<?> startCassandra() {
        CassandraContainer<?> cassandra =
                CassandraContainer.createCassandraContainer(CASSANDRA_IMAGE, network, CASSANDRA_FAMILY, 1, CASSANDRA_FAMILY);
        if ("dse4".equals(CASSANDRA_FAMILY)) {
            cassandra = cassandra.withEnv("DC", CassandraContainer.LOCAL_DC)
                    .withContainerConfigLocation("/config");
        }
        cassandra.start();
        return cassandra;
    }

    @Test
    public void testBackfillSinglePkToKafka() throws Exception {
        try (CassandraContainer<?> cassandra = startCassandra()) {
            try (CqlSession session = cassandra.getCqlSession()) {
                session.execute("CREATE KEYSPACE IF NOT EXISTS ks1 WITH replication = "
                        + "{'class':'SimpleStrategy','replication_factor':'1'};");
                // cdc is disabled: backfill must publish the historical rows itself.
                session.execute("CREATE TABLE IF NOT EXISTS ks1.table1 (id text PRIMARY KEY, a int) WITH cdc=false");
                for (int i = 1; i <= 10; i++) {
                    session.execute(String.format(Locale.ROOT, "INSERT INTO ks1.table1 (id, a) VALUES('%d',1)", i));
                }
            }

            runBackfill(cassandra, "ks1", "table1");

            List<ConsumerRecord<byte[], byte[]>> events = poll("events-ks1.table1", 10, "backfill-events");
            assertEquals(10, events.size(), "backfill should publish one event per row to the events topic");
            for (ConsumerRecord<byte[], byte[]> e : events) {
                assertNotNull(e.key(), "event key (primary key) should not be null");
            }

            runSink(sinkConfig(cassandra, "ks1", "table1", "key-value-avro"), events);

            List<ConsumerRecord<byte[], byte[]>> data = poll("data-ks1.table1", 10, "backfill-data");
            assertEquals(10, data.size(), "sink should publish one row per back-filled mutation to the data topic");
            for (ConsumerRecord<byte[], byte[]> r : data) {
                assertNotNull(r.key(), "data record key (primary key) should not be null");
                assertNotNull(r.value(), "data record value (row) should not be null for an insert");
            }
        }
    }

    /**
     * Run the backfill CLI as the standalone shadow JAR with the Kafka provider, exactly as a user
     * would. Blocks until the process completes.
     */
    private void runBackfill(CassandraContainer<?> cassandra, String ksName, String tableName) throws Exception {
        String cdcBackfillBuildDir = System.getProperty("cdcBackfillBuildDir");
        String projectVersion = System.getProperty("projectVersion");
        String jarFile = String.format(Locale.ROOT, "backfill-cli-%s-all.jar", projectVersion);
        String jarPath = String.format(Locale.ROOT, "%s/libs/%s", cdcBackfillBuildDir, jarFile);

        ProcessBuilder pb = new ProcessBuilder("java", "-jar", jarPath,
                "--messaging-provider", "kafka",
                "--kafka-bootstrap-servers", kafkaContainer.getBootstrapServers(),
                "--data-dir", dataDir.toString(),
                "--dsbulk-log-dir", logsDir.toString(),
                "--export-host", cassandra.getCqlHostAddress(),
                "--keyspace", ksName,
                "--table", tableName,
                "--export-consistency", "LOCAL_QUORUM");
        log.info("Running backfill command: {}", pb.command());

        Process proc = pb.start();
        boolean finished = proc.waitFor(120, TimeUnit.SECONDS);

        // Surface the subprocess output in the test logs (java 11 has no proc.inputReader()).
        new BufferedReader(new InputStreamReader(proc.getInputStream(), StandardCharsets.UTF_8)).lines()
                .forEach(log::info);
        new BufferedReader(new InputStreamReader(proc.getErrorStream(), StandardCharsets.UTF_8)).lines()
                .forEach(log::error);

        if (!finished) {
            proc.destroy();
            throw new RuntimeException("Backfilling process did not finish in 120 seconds");
        }
        assertEquals(0, proc.exitValue(), "backfill process should exit 0");
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
}
