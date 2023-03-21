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

import com.datastax.oss.cdc.CassandraSourceConnectorConfig;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.dsbulk.tests.utils.FileUtils;
import com.datastax.testcontainers.PulsarContainer;
import com.datastax.testcontainers.cassandra.CassandraContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.shade.org.apache.avro.Conversion;
import org.apache.pulsar.shade.org.apache.avro.LogicalType;
import org.apache.pulsar.shade.org.apache.avro.Schema;
import org.apache.pulsar.shade.org.apache.avro.SchemaBuilder;
import org.apache.pulsar.shade.org.apache.avro.generic.GenericArray;
import org.apache.pulsar.shade.org.apache.avro.generic.GenericData;
import org.apache.pulsar.shade.org.apache.avro.generic.GenericRecordBuilder;
import org.apache.pulsar.shade.org.apache.avro.generic.IndexedRecord;
import org.apache.pulsar.shade.org.apache.avro.util.Utf8;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.datastax.oss.cdc.DataSpec.dataSpecMap;
import static org.junit.jupiter.api.Assertions.*;

@Slf4j
public class BackfillCLIE2ETests {

    public static final DockerImageName CASSANDRA_IMAGE = DockerImageName.parse(
            Optional.ofNullable(System.getenv("CASSANDRA_IMAGE"))
                    .orElse("cassandra:" + System.getProperty("cassandraVersion"))
    ).asCompatibleSubstituteFor("cassandra");

    public static final DockerImageName PULSAR_IMAGE = DockerImageName.parse(
            Optional.ofNullable(System.getenv("PULSAR_IMAGE"))
                    .orElse(System.getProperty("testPulsarImage") + ":" + System.getProperty("testPulsarImageTag"))
    ).asCompatibleSubstituteFor("pulsar");

    private static Network testNetwork;
    private static PulsarContainer<?> pulsarContainer;
    private static CassandraContainer<?> cassandraContainer1;
    private static CassandraContainer<?> cassandraContainer2;
    private Path dataDir;
    private Path logsDir;

    @BeforeAll
    public static void initBeforeClass() throws Exception {
        testNetwork = Network.newNetwork();

        String connectorBuildDir = System.getProperty("connectorBuildDir");
        String projectVersion = System.getProperty("projectVersion");
        String connectorJarFile = String.format(Locale.ROOT, "pulsar-cassandra-source-%s.nar", projectVersion);
        String backfillNarFile =  String.format(Locale.ROOT, "pulsar-cassandra-admin-%s.nar", projectVersion);
        pulsarContainer = new PulsarContainer<>(PULSAR_IMAGE)
                .withNetwork(testNetwork)
                .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd.withName("pulsar"))
                .withFunctionsWorker()
                .withFileSystemBind(
                        String.format(Locale.ROOT, "%s/libs/%s", connectorBuildDir, connectorJarFile),
                        String.format(Locale.ROOT, "/pulsar/connectors/%s", connectorJarFile))
                .withFileSystemBind(
                        String.format(Locale.ROOT, "%s/libs/%s", connectorBuildDir, backfillNarFile),
                        String.format(Locale.ROOT, "/pulsar/connectors/%s", backfillNarFile))
                .withClasspathResourceMapping("client.conf",
                        "/pulsar/conf/client.conf",
                        BindMode.READ_ONLY)
                .withStartupTimeout(Duration.ofSeconds(60));
        pulsarContainer.start();

        // ./pulsar-admin namespaces set-auto-topic-creation public/default --enable --type partitioned --num-partitions 1
        Container.ExecResult result = pulsarContainer.execInContainer(
                "/pulsar/bin/pulsar-admin", "namespaces", "set-auto-topic-creation", "public/default", "--enable");
        assertEquals(0, result.getExitCode(), result.getStdout());
        result = pulsarContainer.execInContainer(
                "/pulsar/bin/pulsar-admin", "namespaces", "set-is-allow-auto-update-schema", "public/default", "--enable");
        assertEquals(0, result.getExitCode(), result.getStdout());

        String pulsarServiceUrl = "pulsar://pulsar:" + pulsarContainer.BROKER_PORT;
        String cassandraFamily = System.getProperty("cassandraFamily");
        String agentName =  "agent-" + cassandraFamily;
        String agentBuildDir = System.getProperty("agentBuildDir");
        log.info("cassandraFamily: {}, agentName: {}, agentBuildDir: {}", cassandraFamily, agentName, agentBuildDir);
        try {
            cassandraContainer1 = CassandraContainer.createCassandraContainerWithAgent(
                    CASSANDRA_IMAGE, testNetwork, cassandraFamily, 1, agentBuildDir, agentName,
                    String.format("pulsarServiceUrl=%s,cdcWorkingDir=/var/lib/cassandra/cdc", pulsarServiceUrl),
                    cassandraFamily);

            // Connector requires 2 nodes to work
            cassandraContainer2 = CassandraContainer.createCassandraContainerWithAgent(
                    CASSANDRA_IMAGE, testNetwork, cassandraFamily, 2, agentBuildDir, agentName,
                    String.format("pulsarServiceUrl=%s,cdcWorkingDir=/var/lib/cassandra/cdc", pulsarServiceUrl),
                    cassandraFamily);

            if ("dse4".equals(cassandraFamily)) {
                cassandraContainer1 = cassandraContainer1.withEnv("DC", CassandraContainer.LOCAL_DC)
                        .withContainerConfigLocation("/config");
                cassandraContainer2 = cassandraContainer2.withEnv("DC", CassandraContainer.LOCAL_DC)
                        .withContainerConfigLocation("/config");
            }

            cassandraContainer1.start();
            cassandraContainer2.start();
        } catch (Exception e) {
            log.error("Failed to create cassandra container", e);
            throw e;
        }
    }

    @BeforeEach
    public void init() throws IOException {
        // create temp dirs
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
        cassandraContainer1.close();
        pulsarContainer.close();
    }

    @Test
    public void testBackfillCLISinglePk() throws InterruptedException, IOException {
        testBackfillCLISinglePk("ks1");
    }

    @Test
    public void testBackfillCLIFullSchema() throws InterruptedException, IOException {
        testBackfillCLIFullSchema("ks1");
    }

    void deployConnector(String ksName, String tableName) throws IOException, InterruptedException {
        String config = String.format(Locale.ROOT, "{\"%s\":\"%s\", \"%s\":\"%s\", \"%s\":\"%s\", \"%s\":\"%s\", \"%s\": \"%s\", \"%s\":\"%s\", \"%s\":\"%s\"}",
                CassandraSourceConnectorConfig.CONTACT_POINTS_OPT, "cassandra-1",
                CassandraSourceConnectorConfig.DC_OPT, "datacenter1",
                CassandraSourceConnectorConfig.KEYSPACE_NAME_CONFIG, ksName,
                CassandraSourceConnectorConfig.TABLE_NAME_CONFIG, tableName,
                CassandraSourceConnectorConfig.EVENTS_TOPIC_NAME_CONFIG, "persistent://public/default/events-" + ksName + "." + tableName,
                CassandraSourceConnectorConfig.EVENTS_SUBSCRIPTION_NAME_CONFIG, "sub1",
                CassandraSourceConnectorConfig.OUTPUT_FORMAT, "key-value-avro");
        Container.ExecResult result = pulsarContainer.execInContainer(
                "/pulsar/bin/pulsar-admin",
                "source", "create",
                "--source-type", "cassandra-source",
                "--tenant", "public",
                "--namespace", "default",
                "--name", "cassandra-source-" + ksName + "-" + tableName,
                "--destination-topic-name", "data-" + ksName + "." + tableName,
                "--source-config ", config);
        assertEquals(0, result.getExitCode(), "deployConnector failed:" + result.getStdout());
    }

    void undeployConnector(String ksName, String tableName) throws IOException, InterruptedException {
        Container.ExecResult result = pulsarContainer.execInContainer(
                "/pulsar/bin/pulsar-admin", "source", "delete",
                "--tenant", "public",
                "--namespace", "default",
                "--name", "cassandra-source-" + ksName + "-" + tableName);
    }

    public void testBackfillCLISinglePk(String ksName) throws InterruptedException, IOException {
        try {
            try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS " + ksName +
                        " WITH replication = {'class':'SimpleStrategy','replication_factor':'2'};");
                // make sure cdc is disabled
                cqlSession.execute("CREATE TABLE IF NOT EXISTS " + ksName + ".table1 (id text PRIMARY KEY, a int) WITH cdc=false");

                // insert 100 columns
                for (int cols = 1; cols <= 100; cols++) {
                    cqlSession.execute(String.format("INSERT INTO %s.table1 (id, a) VALUES('%s',1)", ksName, cols));
                }
            }
            // although CDC is disabled, back-filling depends on the connector to send the back-filled mutations to the
            // date topic
            deployConnector(ksName, "table1");
            runBackfillAsync(ksName, "table1");

            try (PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(pulsarContainer.getPulsarBrokerUrl()).build()) {
                Map<String, Integer> mutationTable1 = new HashMap<>();
                try (Consumer<GenericRecord> consumer = pulsarClient.newConsumer(org.apache.pulsar.client.api.Schema.AUTO_CONSUME())
                        .topic(String.format(Locale.ROOT, "data-%s.table1", ksName))
                        .subscriptionName("sub1")
                        .subscriptionType(SubscriptionType.Key_Shared)
                        .subscriptionMode(SubscriptionMode.Durable)
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                        .subscribe()) {
                    Message<GenericRecord> msg;
                    while ((msg = consumer.receive(90, TimeUnit.SECONDS)) != null &&
                            mutationTable1.values().stream().count() < 100) {
                        GenericRecord record = msg.getValue();
                        assertEquals(SchemaType.KEY_VALUE, record.getSchemaType());
                        GenericRecord key = getKey(msg);
                        GenericRecord value = getValue(record);
                        assertEquals((Integer) 0, mutationTable1.computeIfAbsent(getAndAssertKeyFieldAsString(key, "id"), k -> 0));
                        assertEquals(1, value.getField("a"));
                        mutationTable1.compute(getAndAssertKeyFieldAsString(key, "id"), (k, v) -> v + 1);
                        consumer.acknowledge(msg);
                    }
                    // 100 messages should've been received
                    assertEquals(100, mutationTable1.values().stream().count());
                    for (int cols = 1; cols <= 100; cols++) {
                        assertEquals((Integer) 1, mutationTable1.get(String.valueOf(cols)));
                    }

                    // make sure no more messages are received
                    while ((msg = consumer.receive(30, TimeUnit.SECONDS)) != null) {
                        Object key = getKey(msg);
                        fail("Received more messages than expected. Unwanted key: " + key);
                    }
                }
            }
        } finally {
            dumpFunctionLogs("cassandra-source-" + ksName + "-table1");
            undeployConnector(ksName, "table1");
        }
    }

    public void testBackfillCLIFullSchema(String ksName) throws InterruptedException, IOException {
        try {
            try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS " + ksName +
                        " WITH replication = {'class':'SimpleStrategy','replication_factor':'2'};");

                // Primary key fields are prefixed with 'x'
                // Non-primary key fields are prefixed with 'y'. Only one field is sufficient for the test because the
                // full schema types are e2e tested in the connector project
                cqlSession.execute("CREATE TABLE IF NOT EXISTS " + ksName + ".table2 (" +
                        "xtext text, xascii ascii, xboolean boolean, xblob blob, xtimestamp timestamp, xtime time, xdate date, xuuid uuid, xtimeuuid timeuuid, xtinyint tinyint, xsmallint smallint, xint int, xbigint bigint, xvarint varint, xdecimal decimal, xdouble double, xfloat float, xinet4 inet, xinet6 inet, " +
                        "ytext text," +
                        "primary key (xtext, xascii, xboolean, xblob, xtimestamp, xtime, xdate, xuuid, xtimeuuid, xtinyint, xsmallint, xint, xbigint, xvarint, xdecimal, xdouble, xfloat, xinet4, xinet6)) " +
                        "WITH CLUSTERING ORDER BY (xascii ASC, xboolean DESC, xblob ASC, xtimestamp DESC, xtime DESC, xdate ASC, xuuid DESC, xtimeuuid ASC, xtinyint DESC, xsmallint ASC, xint DESC, xbigint ASC, xvarint DESC, xdecimal ASC, xdouble DESC, xfloat ASC, xinet4 ASC, xinet6 DESC) " +
                        "AND cdc=false"); // make sure cdc is disabled

                cqlSession.execute("INSERT INTO " + ksName + ".table2 (" +
                                "xtext, xascii, xboolean, xblob, xtimestamp, xtime, xdate, xuuid, xtimeuuid, xtinyint, xsmallint, xint, xbigint, xvarint, xdecimal, xdouble, xfloat, xinet4, xinet6, " +
                                "ytext" +
                                ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, ?)",
                        dataSpecMap.get("text").cqlValue,
                        dataSpecMap.get("ascii").cqlValue,
                        dataSpecMap.get("boolean").cqlValue,
                        dataSpecMap.get("blob").cqlValue,
                        dataSpecMap.get("timestamp").cqlValue,
                        dataSpecMap.get("time").cqlValue,
                        dataSpecMap.get("date").cqlValue,
                        dataSpecMap.get("uuid").cqlValue,
                        dataSpecMap.get("timeuuid").cqlValue,
                        dataSpecMap.get("tinyint").cqlValue,
                        dataSpecMap.get("smallint").cqlValue,
                        dataSpecMap.get("int").cqlValue,
                        dataSpecMap.get("bigint").cqlValue,
                        dataSpecMap.get("varint").cqlValue,
                        dataSpecMap.get("decimal").cqlValue,
                        dataSpecMap.get("double").cqlValue,
                        dataSpecMap.get("float").cqlValue,
                        dataSpecMap.get("inet4").cqlValue,
                        dataSpecMap.get("inet6").cqlValue,

                        dataSpecMap.get("text").cqlValue
                );
            }
            // although CDC is disabled, back-filling depends on the connector to send the back-filled mutations to the
            // date topic
            deployConnector(ksName, "table2");
            runBackfillAsync(ksName, "table2");

            try (PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(pulsarContainer.getPulsarBrokerUrl()).build()) {
                try (Consumer<GenericRecord> consumer = pulsarClient.newConsumer(org.apache.pulsar.client.api.Schema.AUTO_CONSUME())
                        .topic(String.format(Locale.ROOT, "data-%s.table2", ksName))
                        .subscriptionName("sub1")
                        .subscriptionType(SubscriptionType.Key_Shared)
                        .subscriptionMode(SubscriptionMode.Durable)
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                        .subscribe()) {
                    int mutationTable2Count = 0;
                    Message<GenericRecord> msg;
                    while ((msg = consumer.receive(120, TimeUnit.SECONDS)) != null && mutationTable2Count < 1) {
                        GenericRecord genericRecord = msg.getValue();
                        mutationTable2Count++;
                        assertEquals(SchemaType.KEY_VALUE, genericRecord.getSchemaType());
                        GenericRecord key = getKey(msg);
                        GenericRecord value = getValue(genericRecord);

                        // check primary key fields
                        Map<String, Object> keyMap = genericRecordToMap(key);
                        for (String fieldName : getKeyFields(key)) {
                            assertField(fieldName, keyMap.get(fieldName));
                        }

                        // check regular columns.
                        Map<String, Object> valueMap = genericRecordToMap(value);
                        for (Field field : value.getFields()) {
                            assertField(field.getName(), valueMap.get(field.getName()));
                        }

                        consumer.acknowledge(msg);
                    }
                    assertEquals(1, mutationTable2Count);

                    // make sure no more messages are received
                    while ((msg = consumer.receive(30, TimeUnit.SECONDS)) != null) {
                        Object key = getKey(msg);
                        fail("Received more messages than expected. Unwanted key: " + key);
                    }
                }
            }
        } finally {
            dumpFunctionLogs("cassandra-source-" + ksName + "-table2");
            undeployConnector(ksName, "table2");
        }
    }

    private void runBackfillAsync(String ksName, String tableName) {
        new Thread(() -> {
            try {
                String[] backfillCommand = new String[] {
                        "/pulsar/bin/pulsar-admin", "cassandra-cdc", "backfill", "--data-dir", dataDir.toString(),
                        "--export-host", cassandraContainer1.getCqlHostAddress(), "--keyspace", ksName, "--table",
                        tableName, "--export-consistency", "LOCAL_QUORUM"
                };
                log.info("Running backfill command: {} ", Arrays.toString(backfillCommand));
                Container.ExecResult result = pulsarContainer.execInContainer(backfillCommand);
                assertEquals(0, result.getExitCode(), "backfill command failed:" + result.getStdout());
                log.info("backfill command finished successfully");
            } catch (InterruptedException | IOException e) {
                log.error("Failed to run backfilling", e);
                throw new RuntimeException(e);
            }
        }).start();
    }

    void assertGenericMap(String field, Map<Utf8, Object> gm) {
        switch (field) {
            case "map":
                log.debug("field={} gm={}", field, gm);
                Map<String, Object> expectedMap = (Map<String, Object>) dataSpecMap.get("map").avroValue;
                Assert.assertEquals(expectedMap.size(), gm.size());
                // convert AVRO Utf8 keys to String.
                Map<String, Object> actualMap = gm.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue()));
                for (Map.Entry<String, Object> entry : expectedMap.entrySet())
                    Assert.assertEquals(expectedMap.get(entry.getKey()), actualMap.get(entry.getKey()));
                return;
        }
        Assert.assertTrue("Unexpected field=" + field, false);
    }

    void assertGenericArray(String field, GenericArray ga) {
        switch (field) {
            case "set": {
                Set set = (Set) dataSpecMap.get("set").avroValue;
                for (Object x : ga)
                    Assert.assertTrue(set.contains(x));
                return;
            }
            case "list": {
                List list = (List) dataSpecMap.get("list").avroValue;
                for (int i = 0; i < ga.size(); i++)
                    // AVRO deserialized as Utf8
                    Assert.assertEquals(list.get(i), ga.get(i).toString());
                return;
            }
            case "listofmap": {
                List list = (List) dataSpecMap.get("listofmap").avroValue;
                for (int i = 0; i < ga.size(); i++) {
                    Map<String, Object> expectedMap = (Map<String, Object>) list.get(i);
                    Map<Utf8, Object> gm = (Map<Utf8, Object>) ga.get(i);
                    Assert.assertEquals(expectedMap.size(), gm.size());
                    // convert AVRO Utf8 keys to String.
                    Map<String, Object> actualMap = gm.entrySet().stream().collect(Collectors.toMap(
                            e -> e.getKey().toString(),
                            e -> e.getValue()));
                    for (Map.Entry<String, Object> entry : expectedMap.entrySet())
                        Assert.assertEquals(expectedMap.get(entry.getKey()), actualMap.get(entry.getKey()));
                }
                return;
            }
            case "setofudt": {
                for (int i = 0; i < ga.size(); i++) {
                    GenericData.Record gr = (GenericData.Record) ga.get(i);
                    for (Schema.Field f : gr.getSchema().getFields()) {
                        assertField(f.name(), gr.get(f.name()));
                    }
                }
                return;
            }
        }
        Assert.assertTrue("Unexpected field=" + field, false);
    }

    void assertField(String fieldName, Object value) {
        String vKey = fieldName.substring(1);
        if (!vKey.equals("udt") && !vKey.equals("udtoptional") && !vKey.equals("setofudt")) {
            Assert.assertTrue("Unknown field " + vKey, dataSpecMap.containsKey(vKey));
        }
        if (value instanceof GenericRecord) {
            assertGenericRecords(vKey, (GenericRecord) value);
        } else if (value instanceof Collection) {
            assertGenericArray(vKey, (GenericData.Array) value);
        } else if (value instanceof Map) {
            assertGenericMap(vKey, (Map) value);
        } else if (value instanceof Utf8) {
            // convert Utf8 to String
            Assert.assertEquals("Wrong value for regular field " + fieldName, dataSpecMap.get(vKey).avroValue, value.toString());
        } else if (value instanceof GenericData.Record) {
            GenericData.Record gr = (GenericData.Record) value;
            if (CqlLogicalTypes.CQL_DECIMAL.equals(gr.getSchema().getName())) {
                CqlLogicalTypes.CqlDecimalConversion cqlDecimalConversion = new CqlLogicalTypes.CqlDecimalConversion();
                value = cqlDecimalConversion.fromRecord(gr, gr.getSchema(), CqlLogicalTypes.CQL_DECIMAL_LOGICAL_TYPE);
            } else if (CqlLogicalTypes.CQL_VARINT.equals(gr.getSchema().getName())) {
                CqlLogicalTypes.CqlVarintConversion cqlVarintConversion = new CqlLogicalTypes.CqlVarintConversion();
                value = cqlVarintConversion.fromRecord(gr, gr.getSchema(), CqlLogicalTypes.CQL_VARINT_LOGICAL_TYPE);
            } else if (CqlLogicalTypes.CQL_DURATION.equals(gr.getSchema().getName())) {
                CqlLogicalTypes.CqlDurationConversion cqlDurationConversion = new CqlLogicalTypes.CqlDurationConversion();
                value = cqlDurationConversion.fromRecord(gr, gr.getSchema(), CqlLogicalTypes.CQL_DURATION_LOGICAL_TYPE);
            }
            Assert.assertEquals("Wrong value for regular field " + fieldName, dataSpecMap.get(vKey).avroValue, value);
        } else {
            Assert.assertEquals("Wrong value for regular field " + fieldName, dataSpecMap.get(vKey).avroValue, value);
        }
    }

    void assertGenericRecords(String field, GenericRecord gr) {
        switch (field) {
            case "decimal": {
                ByteBuffer bb = (ByteBuffer) gr.getField(CqlLogicalTypes.CQL_DECIMAL_BIGINT);
                byte[] bytes = new byte[bb.remaining()];
                bb.duplicate().get(bytes);
                BigInteger bigInteger = new BigInteger(bytes);
                BigDecimal bigDecimal = new BigDecimal(bigInteger, (int) gr.getField(CqlLogicalTypes.CQL_DECIMAL_SCALE));
                Assert.assertEquals("Wrong value for field " + field, dataSpecMap.get(field).avroValue, bigDecimal);
            }
            return;
            case "duration": {
                Assert.assertEquals("Wrong value for field " + field, dataSpecMap.get(field).avroValue,
                        CqlDuration.newInstance(
                                (int) gr.getField(CqlLogicalTypes.CQL_DURATION_MONTHS),
                                (int) gr.getField(CqlLogicalTypes.CQL_DURATION_DAYS),
                                (long) gr.getField(CqlLogicalTypes.CQL_DURATION_NANOSECONDS)));
            }
            return;
            case "udt": {
                for (Field f : gr.getFields()) {
                    assertField(f.getName(), gr.getField(f.getName()));
                }
            }
            return;
            case "udtoptional": {
                for (Field f : gr.getFields()) {
                    if (f.getName().equals("ztext")) {
                        assertField(f.getName(), gr.getField(f.getName()));
                    } else {
                        assertNull(gr.getField(f.getName()));
                    }
                }
            }
            return;
        }
        Assert.assertTrue("Unexpected field=" + field, false);
    }

    private List<String> getKeyFields(Object key) {
        return ((GenericRecord) key).getFields().stream().map(f->f.getName()).collect(Collectors.toList());
    }

    static String genericRecordToString(GenericRecord genericRecord) {
        StringBuilder sb = new StringBuilder("{");
        for (Field field : genericRecord.getFields()) {
            if (genericRecord.getField(field) != null) {
                if (sb.length() > 1)
                    sb.append(",");
                sb.append(field.getName()).append("=");
                if (genericRecord.getField(field) instanceof GenericRecord) {
                    sb.append(genericRecordToString((GenericRecord) genericRecord.getField(field)));
                } else {
                    sb.append(genericRecord.getField(field).toString());
                }
            }
        }
        return sb.append("}").toString();
    }

    Map<String, Object> genericRecordToMap(GenericRecord genericRecord) {
        Map<String, Object> map = new HashMap<>();
        for (Field field : genericRecord.getFields()) {
            map.put(field.getName(), genericRecord.getField(field));
        }
        return map;
    }

    private GenericRecord getKey(Message<GenericRecord> msg) {
        Object nativeObject = msg.getValue().getNativeObject();
        return ((KeyValue<GenericRecord, GenericRecord>)nativeObject).getKey();
    }

    private String getAndAssertKeyFieldAsString(GenericRecord key, String fieldName) {
        assertTrue(key.getField(fieldName) instanceof String);
        return (String) key.getField(fieldName);
    }

    private GenericRecord getValue(GenericRecord genericRecord) {
        return (genericRecord.getNativeObject() instanceof KeyValue) ?
                ((KeyValue<GenericRecord, GenericRecord>) genericRecord.getNativeObject()).getValue() :
                genericRecord;
    }

    protected void dumpFunctionLogs(String name) {
        try {
            String logFile = "/pulsar/logs/functions/public/default/" + name + "/" + name + "-0.log";
            String logs = pulsarContainer.<String>copyFileFromContainer(logFile, (inputStream) -> {
                return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
            });
            log.info("Function {} logs {}", name, logs);
        } catch (Throwable err) {
            log.info("Cannot download {} logs", name, err);
        }
    }

    private static class CqlLogicalTypes {

        public static final String CQL_VARINT = "cql_varint";
        public static final CqlVarintLogicalType CQL_VARINT_LOGICAL_TYPE = new CqlVarintLogicalType();
        public static final Schema varintType = CQL_VARINT_LOGICAL_TYPE.addToSchema(Schema.create(Schema.Type.BYTES));

        public static final String CQL_DECIMAL = "cql_decimal";
        public static final String CQL_DECIMAL_BIGINT = "bigint";
        public static final String CQL_DECIMAL_SCALE = "scale";
        public static final CqlDecimalLogicalType CQL_DECIMAL_LOGICAL_TYPE = new CqlDecimalLogicalType();
        public static final Schema decimalType = CQL_DECIMAL_LOGICAL_TYPE.addToSchema(
                SchemaBuilder.record(CQL_DECIMAL)
                        .fields()
                        .name(CQL_DECIMAL_BIGINT).type().bytesType().noDefault()
                        .name(CQL_DECIMAL_SCALE).type().intType().noDefault()
                        .endRecord()
        );

        public static final String CQL_DURATION = "cql_duration";
        public static final String CQL_DURATION_MONTHS = "months";
        public static final String CQL_DURATION_DAYS = "days";
        public static final String CQL_DURATION_NANOSECONDS = "nanoseconds";
        public static final CqlDurationLogicalType CQL_DURATION_LOGICAL_TYPE = new CqlDurationLogicalType();
        public static final Schema durationType = CQL_DURATION_LOGICAL_TYPE.addToSchema(
                SchemaBuilder.record(CQL_DURATION)
                        .fields()
                        .name(CQL_DURATION_MONTHS).type().intType().noDefault()
                        .name(CQL_DURATION_DAYS).type().intType().noDefault()
                        .name(CQL_DURATION_NANOSECONDS).type().longType().noDefault()
                        .endRecord()
        );

        private static class CqlDurationLogicalType extends LogicalType {
            public CqlDurationLogicalType() {
                super(CQL_DURATION);
            }

            @Override
            public void validate(Schema schema) {
                super.validate(schema);
                // validate the type
                if (schema.getType() != Schema.Type.RECORD) {
                    throw new IllegalArgumentException("Logical type cql_duration must be backed by a record");
                }
            }
        }

        public static class CqlVarintLogicalType extends LogicalType {
            public CqlVarintLogicalType() {
                super(CQL_VARINT);
            }

            @Override
            public void validate(Schema schema) {
                super.validate(schema);
                // validate the type
                if (schema.getType() != Schema.Type.BYTES) {
                    throw new IllegalArgumentException("Logical type cql_varint must be backed by bytes");
                }
            }
        }

        public static class CqlDecimalLogicalType extends LogicalType {
            public CqlDecimalLogicalType() {
                super(CQL_DECIMAL);
            }

            @Override
            public void validate(Schema schema) {
                super.validate(schema);
                // validate the type
                if (schema.getType() != Schema.Type.RECORD) {
                    throw new IllegalArgumentException("Logical type cql_decimal must be backed by a record");
                }
            }
        }

        private static class CqlVarintConversion extends Conversion<BigInteger> {
            @Override
            public Class<BigInteger> getConvertedType() {
                return BigInteger.class;
            }

            @Override
            public String getLogicalTypeName() {
                return CQL_VARINT;
            }

            @Override
            public BigInteger fromBytes(ByteBuffer value, Schema schema, LogicalType type) {
                byte[] arr = new byte[value.remaining()];
                value.duplicate().get(arr);
                return new BigInteger(arr);
            }

            @Override
            public ByteBuffer toBytes(BigInteger value, Schema schema, LogicalType type) {
                return ByteBuffer.wrap(value.toByteArray());
            }
        }

        private static class CqlDecimalConversion extends Conversion<BigDecimal> {
            @Override
            public Class<BigDecimal> getConvertedType() {
                return BigDecimal.class;
            }

            @Override
            public String getLogicalTypeName() {
                return CQL_DECIMAL;
            }

            @Override
            public BigDecimal fromRecord(IndexedRecord value, Schema schema, LogicalType type) {
                ByteBuffer bb = (ByteBuffer) value.get(0);
                byte[] bytes = new byte[bb.remaining()];
                bb.duplicate().get(bytes);
                int scale = (int) value.get(1);
                return new BigDecimal(new BigInteger(bytes), scale);
            }

            @Override
            public IndexedRecord toRecord(BigDecimal value, Schema schema, LogicalType type) {
                return new GenericRecordBuilder(decimalType)
                        .set(CQL_DECIMAL_BIGINT, ByteBuffer.wrap(value.unscaledValue().toByteArray()))
                        .set(CQL_DECIMAL_SCALE, value.scale())
                        .build();
            }
        }

        public static class CqlDurationConversion extends Conversion<CqlDuration> {
            @Override
            public Class<CqlDuration> getConvertedType() {
                return CqlDuration.class;
            }

            @Override
            public String getLogicalTypeName() {
                return CQL_DURATION;
            }

            @Override
            public CqlDuration fromRecord(IndexedRecord value, Schema schema, LogicalType type) {
                return CqlDuration.newInstance((int) value.get(0), (int) value.get(1), (long) value.get(2));
            }

            @Override
            public IndexedRecord toRecord(CqlDuration value, Schema schema, LogicalType type) {
                org.apache.pulsar.shade.org.apache.avro.generic.GenericRecord record = new GenericData.Record(durationType);
                record.put(CQL_DURATION_MONTHS, value.getMonths());
                record.put(CQL_DURATION_DAYS, value.getDays());
                record.put(CQL_DURATION_NANOSECONDS, value.getNanoseconds());
                return record;
            }
        }
    }
}

