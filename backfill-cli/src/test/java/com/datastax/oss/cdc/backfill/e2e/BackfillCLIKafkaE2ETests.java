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
import com.datastax.oss.cdc.CassandraSourceConnectorConfig;
import com.datastax.oss.cdc.converters.AvroRowConverter;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.dsbulk.tests.utils.FileUtils;
import com.datastax.oss.kafka.source.KafkaCassandraSourceTask;
import com.datastax.testcontainers.cassandra.CassandraContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * E2E test for the Kafka backfill path. Mirrors {@link BackfillCLIE2ETests}: runs the backfill
 * CLI as a subprocess against a real Cassandra node to publish historical rows onto the events
 * topic, then verifies they arrive on the data topic.
 *
 * <p>Unlike the Pulsar version, this doesn't deploy a full Kafka Connect worker - there's no
 * container-friendly equivalent of {@code pulsar-admin source create} for standalone Connect
 * without a REST deployment flow. Instead this drives a real {@link KafkaCassandraSourceTask}
 * in-process through its actual public Connect lifecycle ({@code initialize}/{@code start}/
 * {@code poll}/{@code stop}, with only {@code SourceTaskContext}'s offset lookup mocked out since
 * there's no real Connect worker/offset store backing it here), proving the same events-topic to
 * data-topic pipeline a real worker would run.
 */
@Slf4j
public class BackfillCLIKafkaE2ETests {

    public static final DockerImageName CASSANDRA_IMAGE = DockerImageName.parse(
            Optional.ofNullable(System.getenv("CASSANDRA_IMAGE"))
                    .orElse("cassandra:" + System.getProperty("cassandraVersion"))
    ).asCompatibleSubstituteFor("cassandra");

    private static final String CONTAINER_KAFKA_CONFIG_PATH = "/etc/cassandra/cdc-kafka.conf";

    private static Network testNetwork;
    private static KafkaContainer kafkaContainer;
    private static CassandraContainer<?> cassandraContainer;
    private Path dataDir;
    private Path logsDir;

    @BeforeAll
    public static void initBeforeClass() throws Exception {
        testNetwork = Network.newNetwork();
        kafkaContainer = new KafkaContainer(AgentTestUtil.KAFKA_IMAGE)
                .withNetwork(testNetwork)
                .withNetworkAliases("kafka");
        kafkaContainer.start();

        // Only read by the CDC agent inside the Cassandra container, which never actually fires
        // in this test - the table below is created with cdc=false, same as BackfillCLIE2ETests.
        // The container factory always wires an agent though, so it needs a config file to point
        // at regardless of whether it's exercised.
        File kafkaConf = File.createTempFile("cdc-kafka-agent-", ".conf");
        kafkaConf.deleteOnExit();
        try (FileWriter fw = new FileWriter(kafkaConf)) {
            fw.write("bootstrapServers=kafka:9092\n");
        }

        String cassandraFamily = System.getProperty("cassandraFamily");
        String agentName = "agent-" + cassandraFamily;
        String agentBuildDir = System.getProperty("agentBuildDir");
        String agentParams = String.format(Locale.ROOT,
                "platform=KAFKA,kafkaConfigFile=%s,topicPrefix=events-", CONTAINER_KAFKA_CONFIG_PATH);
        log.info("cassandraFamily: {}, agentName: {}, agentBuildDir: {}", cassandraFamily, agentName, agentBuildDir);
        try {
            cassandraContainer = CassandraContainer.createCassandraContainerWithAgent(
                    CASSANDRA_IMAGE, testNetwork, cassandraFamily, 1, agentBuildDir, agentName, agentParams, cassandraFamily);
            cassandraContainer.withCopyFileToContainer(
                    MountableFile.forHostPath(kafkaConf.getAbsolutePath()), CONTAINER_KAFKA_CONFIG_PATH);
            if ("dse4".equals(cassandraFamily)) {
                cassandraContainer = cassandraContainer.withEnv("DC", CassandraContainer.LOCAL_DC)
                        .withContainerConfigLocation("/config");
            }
            cassandraContainer.start();
        } catch (Exception e) {
            log.error("Failed to create cassandra container", e);
            throw e;
        }
    }

    @BeforeEach
    public void init() throws IOException {
        dataDir = Files.createTempDirectory("data");
        logsDir = Files.createTempDirectory("logs");
    }

    @AfterEach
    void deleteTempDirs() {
        if (dataDir != null && Files.exists(dataDir)) {
            FileUtils.deleteDirectory(dataDir);
        }
        if (logsDir != null && Files.exists(logsDir)) {
            FileUtils.deleteDirectory(logsDir);
        }
    }

    @AfterAll
    public static void closeAfterAll() {
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
    public void testBackfillCLISinglePk() throws Exception {
        final String ksName = "ks1";
        final String tableName = "table1";
        try (CqlSession cqlSession = cassandraContainer.getCqlSession()) {
            cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS " + ksName +
                    " WITH replication = {'class':'SimpleStrategy','replication_factor':'1'};");
            // make sure cdc is disabled
            cqlSession.execute("CREATE TABLE IF NOT EXISTS " + ksName + "." + tableName + " (id text PRIMARY KEY, a int) WITH cdc=false");
            for (int cols = 1; cols <= 100; cols++) {
                cqlSession.execute(String.format(Locale.ROOT, "INSERT INTO %s.%s (id, a) VALUES('%s',1)", ksName, tableName, cols));
            }
        }

        runBackfillJar(ksName, tableName);

        String eventsTopic = "events-" + ksName + "." + tableName;
        String outputTopic = "data-" + ksName + "." + tableName;
        KafkaCassandraSourceTask task = startTask(ksName, tableName, eventsTopic, outputTopic);
        try (CqlSession cqlSession = cassandraContainer.getCqlSession()) {
            KeyspaceMetadata ksm = cqlSession.getMetadata().getKeyspace(ksName).get();
            TableMetadata table = ksm.getTable(tableName).get();

            Map<String, Integer> mutationTable1 = new HashMap<>();
            long deadline = System.currentTimeMillis() + 90_000;
            while (mutationTable1.size() < 100 && System.currentTimeMillis() < deadline) {
                for (SourceRecord record : task.poll()) {
                    assertEquals(outputTopic, record.topic());
                    GenericRecord key = decodeKey(ksm, table, (byte[]) record.key());
                    GenericRecord value = decodeValue(ksm, table, (byte[]) record.value());
                    String id = key.get("id").toString();
                    assertEquals((Integer) 0, mutationTable1.computeIfAbsent(id, k -> 0));
                    assertEquals(1, value.get("a"));
                    mutationTable1.compute(id, (k, v) -> v + 1);
                }
            }

            assertEquals(100, mutationTable1.size());
            for (int cols = 1; cols <= 100; cols++) {
                assertEquals((Integer) 1, mutationTable1.get(String.valueOf(cols)));
            }

            // make sure no more records show up
            List<SourceRecord> extra = task.poll();
            if (!extra.isEmpty()) {
                fail("Received more records than expected: " + extra);
            }
        } finally {
            task.stop();
        }
    }

    private KafkaCassandraSourceTask startTask(String ksName, String tableName, String eventsTopic, String outputTopic) {
        SourceTaskContext context = mock(SourceTaskContext.class);
        OffsetStorageReader offsetReader = mock(OffsetStorageReader.class);
        when(context.offsetStorageReader()).thenReturn(offsetReader);
        when(offsetReader.offset(anyMap())).thenReturn(null);

        Map<String, String> props = ImmutableMap.<String, String>builder()
                .put(CassandraSourceConnectorConfig.KEYSPACE_NAME_CONFIG, ksName)
                .put(CassandraSourceConnectorConfig.TABLE_NAME_CONFIG, tableName)
                .put(CassandraSourceConnectorConfig.EVENTS_TOPIC_NAME_CONFIG, eventsTopic)
                .put(CassandraSourceConnectorConfig.OUTPUT_TOPIC_CONFIG, outputTopic)
                .put(CassandraSourceConnectorConfig.CONTACT_POINTS_OPT, cassandraContainer.getHost())
                .put(CassandraSourceConnectorConfig.PORT_OPT,
                        String.valueOf(cassandraContainer.getMappedPort(CassandraContainer.CQL_PORT)))
                .put(CassandraSourceConnectorConfig.DC_OPT, cassandraContainer.getLocalDc())
                .put(CassandraSourceConnectorConfig.INTERNAL_CONSUMER_BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers())
                // the events topic auto-creates with the broker's default partition count (1 in
                // this test setup, confirmed by KafkaCassandraSourceTaskContainerTests using the
                // same assumption) - a single task owning partition 0 covers all of it.
                .put("internal.consumer.partitions", "0")
                .build();

        KafkaCassandraSourceTask task = new KafkaCassandraSourceTask();
        task.initialize(context);
        task.start(props);
        return task;
    }

    private void runBackfillJar(String ksName, String tableName) throws IOException, InterruptedException {
        File kafkaConfigFile = File.createTempFile("backfill-kafka-", ".properties");
        kafkaConfigFile.deleteOnExit();
        try (FileWriter fw = new FileWriter(kafkaConfigFile)) {
            fw.write("bootstrapServers=" + kafkaContainer.getBootstrapServers() + "\n");
        }

        String cdcBackfillBuildDir = System.getProperty("cdcBackfillBuildDir");
        String projectVersion = System.getProperty("projectVersion");
        String cdcBackfillJarFile = String.format(Locale.ROOT, "backfill-cli-%s-all.jar", projectVersion);
        String cdcBackfillFullJarPath = String.format(Locale.ROOT, "%s/libs/%s", cdcBackfillBuildDir, cdcBackfillJarFile);

        ProcessBuilder pb = new ProcessBuilder("java", "-jar", cdcBackfillFullJarPath,
                "--data-dir", dataDir.toString(), "--dsbulk-log-dir", logsDir.toString(),
                "--export-host", cassandraContainer.getCqlHostAddress(), "--keyspace", ksName, "--table", tableName,
                "--export-consistency", "LOCAL_QUORUM",
                "--platform", "KAFKA", "--kafka-config-file", kafkaConfigFile.getAbsolutePath());

        log.info("Running backfill command: {} ", pb.command());

        Process proc = pb.start();
        boolean finished = proc.waitFor(90, TimeUnit.SECONDS);

        new BufferedReader(new InputStreamReader(proc.getErrorStream(), StandardCharsets.UTF_8)).lines()
                .forEach(log::error);
        new BufferedReader(new InputStreamReader(proc.getInputStream(), StandardCharsets.UTF_8)).lines()
                .forEach(log::info);

        if (!finished) {
            proc.destroy();
            throw new RuntimeException("Backfilling process did not finish in 90 seconds");
        } else if (proc.exitValue() != 0) {
            throw new RuntimeException("Backfilling process failed with exit code " + proc.exitValue());
        }
        log.info("Backfilling process finished successfully");
    }

    private GenericRecord decodeKey(KeyspaceMetadata ksm, TableMetadata table, byte[] bytes) throws IOException {
        org.apache.avro.Schema schema = new AvroRowConverter(ksm, table, table.getPrimaryKey()).nativeSchema;
        return decodeAvroBytes(bytes, schema);
    }

    private GenericRecord decodeValue(KeyspaceMetadata ksm, TableMetadata table, byte[] bytes) throws IOException {
        List<ColumnMetadata> nonPkColumns = table.getColumns().values().stream()
                .filter(c -> !table.getPrimaryKey().contains(c))
                .collect(Collectors.toList());
        org.apache.avro.Schema schema = new AvroRowConverter(ksm, table, nonPkColumns).nativeSchema;
        return decodeAvroBytes(bytes, schema);
    }

    private GenericRecord decodeAvroBytes(byte[] bytes, org.apache.avro.Schema schema) throws IOException {
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        return reader.read(null, decoder);
    }
}
