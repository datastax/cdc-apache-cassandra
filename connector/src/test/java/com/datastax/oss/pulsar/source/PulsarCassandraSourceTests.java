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
package com.datastax.oss.pulsar.source;

import com.datastax.oss.cdc.CassandraSourceConnectorConfig;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.datastax.testcontainers.ChaosNetworkContainer;
import com.datastax.testcontainers.PulsarContainer;
import com.datastax.testcontainers.cassandra.CassandraContainer;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
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
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.ObjectMapper;
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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.datastax.oss.cdc.DataSpec.dataSpecMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@RequiredArgsConstructor
public abstract class PulsarCassandraSourceTests {

    private static final String CQL_VARINT = "cql_varint";
    private static final String CQL_DECIMAL = "cql_decimal";
    private static final String CQL_DURATION = "cql_duration";

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

    private static final ObjectMapper mapper = new ObjectMapper();

    private final String outputFormat;

    private final SchemaType schemaType;

    @BeforeAll
    public static void initBeforeClass() throws Exception {
        testNetwork = Network.newNetwork();

        String connectorBuildDir = System.getProperty("connectorBuildDir");
        String projectVersion = System.getProperty("projectVersion");
        String connectorJarFile = String.format(Locale.ROOT,  "pulsar-cassandra-source-%s.nar", projectVersion);
        pulsarContainer = new PulsarContainer<>(PULSAR_IMAGE)
                .withNetwork(testNetwork)
                .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd.withName("pulsar"))
                .withFunctionsWorker()
                .withFileSystemBind(
                        String.format(Locale.ROOT, "%s/libs/%s", connectorBuildDir, connectorJarFile),
                        String.format(Locale.ROOT, "/pulsar/connectors/%s", connectorJarFile))
                .withStartupTimeout(Duration.ofSeconds(60));
        pulsarContainer.start();

        // ./pulsar-admin namespaces set-auto-topic-creation public/default --enable --type partitioned --num-partitions 1
        Container.ExecResult result = pulsarContainer.execInContainer(
                "/pulsar/bin/pulsar-admin", "namespaces", "set-auto-topic-creation", "public/default", "--enable");
        assertEquals(0, result.getExitCode());
        result = pulsarContainer.execInContainer(
                "/pulsar/bin/pulsar-admin", "namespaces", "set-is-allow-auto-update-schema", "public/default", "--enable");
        assertEquals(0, result.getExitCode());

        String pulsarServiceUrl = "pulsar://pulsar:" + pulsarContainer.BROKER_PORT;
        String agentBuildDir = System.getProperty("agentBuildDir");
        cassandraContainer1 = CassandraContainer.createCassandraContainerWithAgent(
                CASSANDRA_IMAGE, testNetwork, 1, agentBuildDir, "agent-c4",
                "pulsarServiceUrl=" + pulsarServiceUrl, "c4");
        cassandraContainer2 = CassandraContainer.createCassandraContainerWithAgent(
                CASSANDRA_IMAGE, testNetwork, 2, agentBuildDir, "agent-c4",
                "pulsarServiceUrl=" + pulsarServiceUrl, "c4");
        cassandraContainer1.start();
        cassandraContainer2.start();
    }

    @AfterAll
    public static void closeAfterAll() {
        cassandraContainer1.close();
        cassandraContainer2.close();
        pulsarContainer.close();
    }

    @Test
    public void testSinglePk() throws InterruptedException, IOException {
        testSinglePk("ks1");
    }

    @Test
    public void testCompoundPk() throws InterruptedException, IOException {
        testCompoundPk("ks1");
    }

    @Test
    public void testSchema() throws InterruptedException, IOException {
        testSchema("ks1");
    }

    @Test
    public void testStaticColumn() throws InterruptedException, IOException {
        testStaticColumn("ks1");
    }

    @Test
    public void testBatchInsert() throws InterruptedException, IOException {
        testBatchInsert("batchinsert");
    }

    void deployConnector(String ksName, String tableName) throws IOException, InterruptedException {
        String config = String.format(Locale.ROOT, "{\"%s\":\"%s\", \"%s\":\"%s\", \"%s\":\"%s\", \"%s\":\"%s\", \"%s\": \"%s\", \"%s\":\"%s\", \"%s\":\"%s\"}",
                CassandraSourceConnectorConfig.CONTACT_POINTS_OPT, "cassandra-1",
                CassandraSourceConnectorConfig.DC_OPT, "datacenter1",
                CassandraSourceConnectorConfig.KEYSPACE_NAME_CONFIG, ksName,
                CassandraSourceConnectorConfig.TABLE_NAME_CONFIG, tableName,
                CassandraSourceConnectorConfig.EVENTS_TOPIC_NAME_CONFIG, "persistent://public/default/events-" + ksName + "." + tableName,
                CassandraSourceConnectorConfig.EVENTS_SUBSCRIPTION_NAME_CONFIG, "sub1",
                CassandraSourceConnectorConfig.OUTPUT_FORMAT, this.outputFormat);
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

    /**
     * Check the connector is running
     * @param ksName
     * @param tableName
     * @return the number of restart
     * @throws IOException
     * @throws InterruptedException
     */
    int connectorStatus(String ksName, String tableName) throws IOException, InterruptedException {
        Container.ExecResult result = pulsarContainer.execInContainer(
                "/pulsar/bin/pulsar-admin", "source", "status",
                "--name", "cassandra-source-" + ksName + "-" + tableName);
        assertEquals(0, result.getExitCode(), "connectorStatus failed:" + result.getStdout());
        String[] resultLines = result.getStdout().split("\\n");
        for(int i = 0; i < resultLines.length; i++) {
            if (resultLines[i].contains("numRestarts")) {
                String numRestart = resultLines[i].split(":")[1];
                numRestart = numRestart.substring(0, numRestart.length() - 1).trim();
                return Integer.parseInt(numRestart);
            }
        }
        return -1;
    }

    // docker exec -it pulsar cat /pulsar/logs/functions/public/default/cassandra-source-ks1-table1/cassandra-source-ks1-table1-0.log
    public void testSinglePk(String ksName) throws InterruptedException, IOException {
        try {
            try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS " + ksName +
                        " WITH replication = {'class':'SimpleStrategy','replication_factor':'2'};");
                cqlSession.execute("CREATE TABLE IF NOT EXISTS " + ksName + ".table1 (id text PRIMARY KEY, a int) WITH cdc=true");
                cqlSession.execute("INSERT INTO " + ksName + ".table1 (id, a) VALUES('1',1)");
                cqlSession.execute("INSERT INTO " + ksName + ".table1 (id, a) VALUES('2',1)");
                cqlSession.execute("INSERT INTO " + ksName + ".table1 (id, a) VALUES('3',1)");
            }
            deployConnector(ksName, "table1");

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
                            mutationTable1.values().stream().mapToInt(i -> i).sum() < 4) {
                        GenericRecord record = msg.getValue();
                        assertEquals(this.schemaType, record.getSchemaType());
                        Object key = getKey(msg);
                        GenericRecord value = getValue(record);
                        assertEquals((Integer) 0, mutationTable1.computeIfAbsent(getAndAssertKeyFieldAsString(key, "id"), k -> 0));
                        assertEquals(1, value.getField("a"));
                        mutationTable1.compute(getAndAssertKeyFieldAsString(key, "id"), (k, v) -> v + 1);
                        consumer.acknowledge(msg);
                    }
                    assertEquals((Integer) 1, mutationTable1.get("1"));
                    assertEquals((Integer) 1, mutationTable1.get("2"));
                    assertEquals((Integer) 1, mutationTable1.get("3"));

                    // trigger a schema update
                    try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                        cqlSession.execute("ALTER TABLE " + ksName + ".table1 ADD b double");
                        cqlSession.execute("INSERT INTO " + ksName + ".table1 (id,a,b) VALUES('1',1,1.0)");
                    }
                    while ((msg = consumer.receive(90, TimeUnit.SECONDS)) != null &&
                            mutationTable1.values().stream().mapToInt(i -> i).sum() < 5) {
                        GenericRecord record = msg.getValue();
                        assertEquals(this.schemaType, record.getSchemaType());
                        Object key = getKey(msg);
                        GenericRecord value = getValue(record);
                        assertEquals("1", getAndAssertKeyFieldAsString(key, "id"));
                        assertEquals(1, value.getField("a"));
                        assertEquals(1.0D, value.getField("b"));
                        mutationTable1.compute(getAndAssertKeyFieldAsString(key,"id"), (k, v) -> v + 1);
                        consumer.acknowledge(msg);
                    }
                    assertEquals((Integer) 2, mutationTable1.get("1"));
                    assertEquals((Integer) 1, mutationTable1.get("2"));
                    assertEquals((Integer) 1, mutationTable1.get("3"));

                    // delete a row
                    try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                        cqlSession.execute("DELETE FROM " + ksName + ".table1 WHERE id = '1'");
                    }
                    while ((msg = consumer.receive(60, TimeUnit.SECONDS)) != null &&
                            mutationTable1.values().stream().mapToInt(i -> i).sum() < 6) {
                        GenericRecord record = msg.getValue();
                        assertEquals(this.schemaType, record.getSchemaType());
                        Object key = getKey(msg);
                        GenericRecord value = getValue(record);
                        assertEquals("1", getAndAssertKeyFieldAsString(key, "id"));
                        assertNullValue(value);
                        mutationTable1.compute(getAndAssertKeyFieldAsString(key, "id"), (k, v) -> v + 1);
                        consumer.acknowledge(msg);
                    }
                    assertEquals((Integer) 3, mutationTable1.get("1"));
                    assertEquals((Integer) 1, mutationTable1.get("2"));
                    assertEquals((Integer) 1, mutationTable1.get("3"));
                }
            }
        } finally {
            dumpFunctionLogs("cassandra-source-" + ksName + "-table1");
            undeployConnector(ksName, "table1");
        }
    }

    // docker exec -it pulsar cat /pulsar/logs/functions/public/default/cassandra-source-ks1-table2/cassandra-source-ks1-table2-0.log
    public void testCompoundPk(String ksName) throws InterruptedException, IOException {
        try {
            try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS " + ksName +
                        " WITH replication = {'class':'SimpleStrategy','replication_factor':'2'};");
                cqlSession.execute("CREATE TABLE IF NOT EXISTS " + ksName + ".table2 (a text, b int, c int, PRIMARY KEY(a,b)) WITH cdc=true");
                cqlSession.execute("INSERT INTO " + ksName + ".table2 (a,b,c) VALUES('1',1,1)");
                cqlSession.execute("INSERT INTO " + ksName + ".table2 (a,b,c) VALUES('2',1,1)");
                cqlSession.execute("INSERT INTO " + ksName + ".table2 (a,b,c) VALUES('3',1,1)");
            }
            deployConnector(ksName, "table2");
            try (PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(pulsarContainer.getPulsarBrokerUrl()).build()) {
                Map<String, Integer> mutationTable2 = new HashMap<>();
                try (Consumer<GenericRecord> consumer = pulsarClient.newConsumer(org.apache.pulsar.client.api.Schema.AUTO_CONSUME())
                        .topic(String.format(Locale.ROOT, "data-%s.table2", ksName))
                        .subscriptionName("sub1")
                        .subscriptionType(SubscriptionType.Key_Shared)
                        .subscriptionMode(SubscriptionMode.Durable)
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                        .subscribe()) {
                    Message<GenericRecord> msg;
                    while ((msg = consumer.receive(60, TimeUnit.SECONDS)) != null &&
                            mutationTable2.values().stream().mapToInt(i -> i).sum() < 4) {
                        GenericRecord record = msg.getValue();
                        assertEquals(this.schemaType, record.getSchemaType());
                        Object key = getKey(msg);
                        GenericRecord value = getValue(record);
                        assertEquals((Integer) 0, mutationTable2.computeIfAbsent(getAndAssertKeyFieldAsString(key, "a"), k -> 0));
                        assertEquals(1,  getAndAssertKeyFieldAsInt(key, "b"));
                        assertEquals(1, value.getField("c"));
                        mutationTable2.compute(getAndAssertKeyFieldAsString(key, "a"), (k, v) -> v + 1);
                        consumer.acknowledge(msg);
                    }
                    assertEquals((Integer) 1, mutationTable2.get("1"));
                    assertEquals((Integer) 1, mutationTable2.get("2"));
                    assertEquals((Integer) 1, mutationTable2.get("3"));

                    // trigger a schema update
                    try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                        cqlSession.execute("CREATE TYPE " + ksName + ".type2 (a2 int, b2 boolean);");
                        cqlSession.execute("ALTER TABLE " + ksName + ".table2 ADD d type2");
                        cqlSession.execute("INSERT INTO " + ksName + ".table2 (a,b,c,d) VALUES('1',1,1,{a2:1,b2:true})");
                    }
                    while ((msg = consumer.receive(30, TimeUnit.SECONDS)) != null &&
                            mutationTable2.values().stream().mapToInt(i -> i).sum() < 5) {
                        GenericRecord record = msg.getValue();
                        assertEquals(this.schemaType, record.getSchemaType());
                        Object key = getKey(msg);
                        GenericRecord value = getValue(record);
                        assertEquals("1", getAndAssertKeyFieldAsString(key, "a"));
                        assertEquals(1, getAndAssertKeyFieldAsInt(key, "b"));
                        assertEquals(1, value.getField("c"));
                        GenericRecord udtGenericRecord = (GenericRecord) value.getField("d");
                        assertEquals(1, udtGenericRecord.getField("a2"));
                        assertEquals(true, udtGenericRecord.getField("b2"));
                        mutationTable2.compute(getAndAssertKeyFieldAsString(key, "a"), (k, v) -> v + 1);
                        consumer.acknowledge(msg);
                    }
                    assertEquals((Integer) 2, mutationTable2.get("1"));
                    assertEquals((Integer) 1, mutationTable2.get("2"));
                    assertEquals((Integer) 1, mutationTable2.get("3"));

                    // delete a row
                    try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                        cqlSession.execute("DELETE FROM " + ksName + ".table2 WHERE a = '1' AND b = 1");
                    }
                    while ((msg = consumer.receive(30, TimeUnit.SECONDS)) != null &&
                            mutationTable2.values().stream().mapToInt(i -> i).sum() < 6) {
                        GenericRecord record = msg.getValue();
                        assertEquals(this.schemaType, record.getSchemaType());
                        Object key = getKey(msg);
                        GenericRecord value = getValue(record);
                        assertEquals("1", getAndAssertKeyFieldAsString(key,"a"));
                        assertEquals(1, getAndAssertKeyFieldAsInt(key, "b"));
                        assertNullValue(value);
                        mutationTable2.compute(getAndAssertKeyFieldAsString(key, "a"), (k, v) -> v + 1);
                        consumer.acknowledge(msg);
                    }
                    assertEquals((Integer) 3, mutationTable2.get("1"));
                    assertEquals((Integer) 1, mutationTable2.get("2"));
                    assertEquals((Integer) 1, mutationTable2.get("3"));
                }
            }
        } finally {
            dumpFunctionLogs("cassandra-source-" + ksName + "-table2");
            undeployConnector(ksName, "table2");
        }
    }

    // docker exec -it pulsar cat /pulsar/logs/functions/public/default/cassandra-source-ks1-table3/cassandra-source-ks1-table3-0.log
    public void testSchema(String ksName) throws InterruptedException, IOException {
        try {
            try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS " + ksName +
                        " WITH replication = {'class':'SimpleStrategy','replication_factor':'1'};");

                cqlSession.execute("CREATE TYPE IF NOT EXISTS " + ksName + ".zudt (" +
                        "ztext text, zascii ascii, zboolean boolean, zblob blob, ztimestamp timestamp, ztime time, zdate date, zuuid uuid, ztimeuuid timeuuid, " +
                        "ztinyint tinyint, zsmallint smallint, zint int, zbigint bigint, zvarint varint, zdecimal decimal, zduration duration, zdouble double, " +
                        "zfloat float, zinet4 inet, zinet6 inet, zlist frozen<list<text>>, zset frozen<set<int>>, zmap frozen<map<text, double>>" +
                        ");");
                UserDefinedType zudt =
                        cqlSession.getMetadata()
                                .getKeyspace(ksName)
                                .flatMap(ks -> ks.getUserDefinedType("zudt"))
                                .orElseThrow(() -> new IllegalArgumentException("Missing UDT zudt definition"));
                UdtValue zudtValue = zudt.newValue(
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
                        dataSpecMap.get("duration").cqlValue,
                        dataSpecMap.get("double").cqlValue,
                        dataSpecMap.get("float").cqlValue,
                        dataSpecMap.get("inet4").cqlValue,
                        dataSpecMap.get("inet6").cqlValue,
                        dataSpecMap.get("list").cqlValue,
                        dataSpecMap.get("set").cqlValue,
                        dataSpecMap.get("map").cqlValue
                );

                // force udt values to be null by populating 1 item, using zudt.newValue() without explicitly setting
                // any field to non-null value will cause the udt column itself to be null in the C* table
                UdtValue zudtOptionalValues = zudt.newValue(dataSpecMap.get("text").cqlValue);

                cqlSession.execute("CREATE TABLE IF NOT EXISTS " + ksName + ".table3 (" +
                        "xtext text, xascii ascii, xboolean boolean, xblob blob, xtimestamp timestamp, xtime time, xdate date, xuuid uuid, xtimeuuid timeuuid, xtinyint tinyint, xsmallint smallint, xint int, xbigint bigint, xvarint varint, xdecimal decimal, xdouble double, xfloat float, xinet4 inet, xinet6 inet, " +
                        "ytext text, yascii ascii, yboolean boolean, yblob blob, ytimestamp timestamp, ytime time, ydate date, yuuid uuid, ytimeuuid timeuuid, ytinyint tinyint, ysmallint smallint, yint int, ybigint bigint, yvarint varint, ydecimal decimal, ydouble double, yfloat float, yinet4 inet, yinet6 inet, yduration duration, yudt zudt, yudtoptional zudt, ylist list<text>, yset set<int>, ymap map<text, double>, ylistofmap list<frozen<map<text,double>>>, ysetofudt set<frozen<zudt>>," +
                        "primary key (xtext, xascii, xboolean, xblob, xtimestamp, xtime, xdate, xuuid, xtimeuuid, xtinyint, xsmallint, xint, xbigint, xvarint, xdecimal, xdouble, xfloat, xinet4, xinet6)) " +
                        "WITH CLUSTERING ORDER BY (xascii ASC, xboolean DESC, xblob ASC, xtimestamp DESC, xtime DESC, xdate ASC, xuuid DESC, xtimeuuid ASC, xtinyint DESC, xsmallint ASC, xint DESC, xbigint ASC, xvarint DESC, xdecimal ASC, xdouble DESC, xfloat ASC, xinet4 ASC, xinet6 DESC) AND cdc=true");
                cqlSession.execute("INSERT INTO " + ksName + ".table3 (" +
                                "xtext, xascii, xboolean, xblob, xtimestamp, xtime, xdate, xuuid, xtimeuuid, xtinyint, xsmallint, xint, xbigint, xvarint, xdecimal, xdouble, xfloat, xinet4, xinet6, " +
                                "ytext, yascii, yboolean, yblob, ytimestamp, ytime, ydate, yuuid, ytimeuuid, ytinyint, ysmallint, yint, ybigint, yvarint, ydecimal, ydouble, yfloat, yinet4, yinet6, yduration, yudt, yudtoptional, ylist, yset, ymap, ylistofmap, ysetofudt" +
                                ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, ?,?,?, ?,?,?,?,?)",
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

                        dataSpecMap.get("duration").cqlValue,
                        zudtValue,
                        zudtOptionalValues,

                        dataSpecMap.get("list").cqlValue,
                        dataSpecMap.get("set").cqlValue,
                        dataSpecMap.get("map").cqlValue,
                        dataSpecMap.get("listofmap").cqlValue,
                        ImmutableSet.of(zudtValue, zudtValue)
                );
            }
            deployConnector(ksName, "table3");
            try (PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(pulsarContainer.getPulsarBrokerUrl()).build()) {
                try (Consumer<GenericRecord> consumer = pulsarClient.newConsumer(org.apache.pulsar.client.api.Schema.AUTO_CONSUME())
                        .topic(String.format(Locale.ROOT, "data-%s.table3", ksName))
                        .subscriptionName("sub1")
                        .subscriptionType(SubscriptionType.Key_Shared)
                        .subscriptionMode(SubscriptionMode.Durable)
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                        .subscribe()) {
                    int mutationTable3Count = 0;
                    Message<GenericRecord> msg;
                    while ((msg = consumer.receive(120, TimeUnit.SECONDS)) != null && mutationTable3Count < 1) {
                        GenericRecord genericRecord = msg.getValue();
                        mutationTable3Count++;
                        assertEquals(this.schemaType, genericRecord.getSchemaType());
                        Object key = getKey(msg);
                        GenericRecord value = getValue(genericRecord);

                        // check primary key fields
                        Map<String, Object> keyMap = keyToMap(key);
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
                    assertEquals(1, mutationTable3Count);
                }
            }
        } finally {
            dumpFunctionLogs("cassandra-source-" + ksName + "-table3");
            undeployConnector(ksName, "table3");
        }
    }

    void assertGenericMap(String field, Map<Utf8, Object> gm) {
        switch (field) {
            case "map":
                log.debug("field={} gm={}", field, gm);
                Map<String, Object> expectedMap = (Map<String, Object>) dataSpecMap.get("map").avroValue;
                Assert.assertEquals(expectedMap.size(), gm.size());
                // convert AVRO Utf8 keys to String.
                Map<String, Object> actualMap = gm.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue()));
                for(Map.Entry<String, Object> entry : expectedMap.entrySet())
                    Assert.assertEquals(expectedMap.get(entry.getKey()), actualMap.get(entry.getKey()));
                return;
        }
        Assert.assertTrue("Unexpected field="+field, false);
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
                    for(Map.Entry<String, Object> entry : expectedMap.entrySet())
                        Assert.assertEquals(expectedMap.get(entry.getKey()), actualMap.get(entry.getKey()));
                }
                return;
            }
            case "setofudt": {
                for (int i = 0; i < ga.size(); i++) {
                    GenericData.Record gr = (GenericData.Record) ga.get(i);
                    for(Schema.Field f : gr.getSchema().getFields()) {
                        assertField(f.name(), gr.get(f.name()));
                    }
                }
                return;
            }
        }
        Assert.assertTrue("Unexpected field="+field, false);
    }

    void assertField(String fieldName, Object value) {
        String vKey = fieldName.substring(1);
        if (!vKey.equals("udt") && !vKey.equals("udtoptional") && ! vKey.equals("setofudt")) {
            Assert.assertTrue("Unknown field " + vKey, dataSpecMap.containsKey(vKey));
        }
        if (value instanceof GenericRecord) {
            assertGenericRecords(vKey, (GenericRecord) value);
        } else if (value instanceof JsonNode) {
            assertJsonNode(vKey, (JsonNode) value);
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
                CqlLogicalTypes.CqlDurationConversion  cqlDurationConversion = new CqlLogicalTypes.CqlDurationConversion();
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
                    if (f.getName().equals("ztext")){
                        assertField(f.getName(), gr.getField(f.getName()));
                    }
                    else {
                        assertNull(gr.getField(f.getName()));
                    }
                }
            }
            return;
        }
        Assert.assertTrue("Unexpected field="+field, false);
    }

    @SneakyThrows
    void assertJsonNode(String field, JsonNode node) {
        switch (field) {
            case "text":
            case "ascii":
            case "uuid":
            case "timeuuid":
            case "inet4":
            case "inet6": {
                Assert.assertEquals("Wrong value for regular field " + field, dataSpecMap.get(field).jsonValue(), node.asText());
            }
            return;
            case "boolean": {
                Assert.assertEquals("Wrong value for regular field " + field, dataSpecMap.get(field).jsonValue(), node.asBoolean());
            }
            return;
            case "blob":
            case "varint": {
                Assert.assertArrayEquals("Wrong value for regular field " + field, (byte[]) dataSpecMap.get(field).jsonValue(), node.binaryValue());
            }
            return;
            case "timestamp":
            case "time":
            case "date":
            case "tinyint":
            case "smallint":
            case "int":
            case "bigint":
            case "double":
            case "float": {
                Assert.assertEquals("Wrong value for regular field " + field, dataSpecMap.get(field).jsonValue(), node.numberValue());
            }
            return;
            case "set": {
                Assert.assertTrue(node.isArray());
                Set set = (Set) dataSpecMap.get("set").jsonValue();
                for (JsonNode x : node)
                    Assert.assertTrue(set.contains(x.asInt()));
                return;
            }
            case "list": {
                Assert.assertTrue(node.isArray());
                List list = (List) dataSpecMap.get("list").jsonValue();
                int fieldCounter = 0;
                for (JsonNode x : node) {
                    Assert.assertEquals(list.get(fieldCounter), x.asText());
                    fieldCounter++;
                }
                return;
            }
            case "map": {
                Assert.assertTrue(node.isContainerNode());
                Map<String, Object> expectedMap = (Map<String, Object>) dataSpecMap.get("map").jsonValue();
                AtomicInteger size = new AtomicInteger();
                node.fieldNames().forEachRemaining(f -> size.getAndIncrement());
                Assert.assertEquals(expectedMap.size(), size.get());
                for(Map.Entry<String, Object> entry : expectedMap.entrySet())
                    Assert.assertEquals(expectedMap.get(entry.getKey()), node.get(entry.getKey()).asDouble());
                return;
            }
            case "listofmap": {
                List list = (List) dataSpecMap.get("listofmap").jsonValue();
                int fieldCounter = 0;
                for (JsonNode mapNode : node) {
                    Assert.assertTrue(mapNode.isContainerNode());
                    Map<String, Object> expectedMap = (Map<String, Object>) list.get(fieldCounter);
                    AtomicInteger size = new AtomicInteger();
                    mapNode.fieldNames().forEachRemaining(f -> size.getAndIncrement());
                    Assert.assertEquals(expectedMap.size(), size.get());
                    for(Map.Entry<String, Object> entry : expectedMap.entrySet())
                        Assert.assertEquals(expectedMap.get(entry.getKey()), mapNode.get(entry.getKey()).asDouble());
                }
                return;
            }
            case "setofudt": {
                for (JsonNode udtNode : node) {
                    for (Iterator<Map.Entry<String, JsonNode>> it = udtNode.fields(); it.hasNext(); ) {
                        Map.Entry<String, JsonNode> f = it.next();
                        assertField(f.getKey(), f.getValue());
                    }
                }
                return;
            }
            case "decimal": {
                byte[] bytes = node.get(CqlLogicalTypes.CQL_DECIMAL_BIGINT).binaryValue();
                BigInteger bigInteger = new BigInteger(bytes);
                BigDecimal bigDecimal = new BigDecimal(bigInteger, node.get(CqlLogicalTypes.CQL_DECIMAL_SCALE).asInt());
                Assert.assertEquals("Wrong value for field " + field, dataSpecMap.get(field).jsonValue(), bigDecimal);
            }
            return;
            case "duration": {
                Assert.assertEquals("Wrong value for field " + field, dataSpecMap.get(field).jsonValue(),
                        CqlDuration.newInstance(
                                node.get(CqlLogicalTypes.CQL_DURATION_MONTHS).asInt(),
                                node.get(CqlLogicalTypes.CQL_DURATION_DAYS).asInt(),
                                node.get(CqlLogicalTypes.CQL_DURATION_NANOSECONDS).asLong()));
            }
            return;
            case "udt": {
                for (Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
                    Map.Entry<String, JsonNode> f = it.next();
                    assertField(f.getKey(), f.getValue());
                }
            }
            return;
            case "udtoptional": {
                for (Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
                    Map.Entry<String, JsonNode> f = it.next();
                    if (f.getKey().equals("ztext")) {
                        assertField(f.getKey(), f.getValue());
                    } else {
                        assertNull(f.getValue());
                    }
                }
            }
            return;
        }
        Assert.assertTrue("Unexpected field="+field, false);
    }

    public void testBatchInsert(String ksName) throws InterruptedException, IOException {
        try {
            try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS " + ksName +
                        " WITH replication = {'class':'SimpleStrategy','replication_factor':'2'};");
                cqlSession.execute("CREATE TABLE IF NOT EXISTS " + ksName + ".table1 (id text, a int, b int, PRIMARY KEY (id, a)) WITH cdc=true");
            }
            deployConnector(ksName, "table1");

            // run batch insert in parallel
            Executors.newSingleThreadExecutor().submit(() -> {
                try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                    PreparedStatement statement = cqlSession.prepare("INSERT INTO " + ksName + ".table1 (id, a, b) VALUES (?,?,?)");
                    for(int batch = 0; batch < 10; batch++) {
                        BatchStatementBuilder batchBuilder = BatchStatement.builder(BatchType.UNLOGGED);
                        for (int i = 0; i < 1000; i++) {
                            batchBuilder.addStatement(statement.bind("a" + batch, i, i));
                        }
                        cqlSession.execute(batchBuilder.build());
                    }
                    // no drain, test use NRT CDC
                } catch (Exception e) {
                    log.error("error:", e);
                }
            });

            try (PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(pulsarContainer.getPulsarBrokerUrl()).build()) {
                try (Consumer<GenericRecord> consumer = pulsarClient.newConsumer(org.apache.pulsar.client.api.Schema.AUTO_CONSUME())
                        .topic(String.format(Locale.ROOT, "data-%s.table1", ksName))
                        .subscriptionName("sub1")
                        .subscriptionType(SubscriptionType.Key_Shared)
                        .subscriptionMode(SubscriptionMode.Durable)
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                        .subscribe()) {
                    Message<GenericRecord> msg;
                    int msgCount = 0;
                    while ((msg = consumer.receive(90, TimeUnit.SECONDS)) != null && msgCount < 10000) {
                        msgCount++;
                        GenericRecord record = msg.getValue();
                        assertEquals(this.schemaType, record.getSchemaType());
                        Object key = getKey(msg);
                        Assert.assertTrue(getAndAssertKeyFieldAsString(key,  "id").startsWith("a"));
                        consumer.acknowledge(msg);
                    }
                    assertEquals(10000, msgCount);
                }
            }
        } finally {
            dumpFunctionLogs("cassandra-source-" + ksName + "-table1");
            undeployConnector(ksName, "table1");
        }
    }

    @Test
    public void testReadTimeout() throws InterruptedException, IOException {
        final String ksName = "ksx";
        try(ChaosNetworkContainer<?> chaosContainer = new ChaosNetworkContainer<>(cassandraContainer2.getContainerName(), "100s")) {
            try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS " + ksName +
                        " WITH replication = {'class':'SimpleStrategy','replication_factor':'2'};");
                cqlSession.execute("CREATE TABLE IF NOT EXISTS " + ksName + ".table1 (id text PRIMARY KEY, a int) WITH cdc=true");
                cqlSession.execute("INSERT INTO " + ksName + ".table1 (id, a) VALUES('1',1)");

                deployConnector(ksName, "table1");

                chaosContainer.start();
                cqlSession.execute("INSERT INTO " + ksName + ".table1 (id, a) VALUES('2',1)");
                cqlSession.execute("INSERT INTO " + ksName + ".table1 (id, a) VALUES('3',1)");
            }
            try (PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(pulsarContainer.getPulsarBrokerUrl()).build()) {
                try (Consumer<GenericRecord> consumer = pulsarClient.newConsumer(org.apache.pulsar.client.api.Schema.AUTO_CONSUME())
                        .topic(String.format(Locale.ROOT, "data-%s.table1", ksName))
                        .subscriptionName("sub1")
                        .subscriptionType(SubscriptionType.Key_Shared)
                        .subscriptionMode(SubscriptionMode.Durable)
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                        .subscribe()) {
                    Message<GenericRecord> msg;
                    int numMessage = 0;
                    while ((msg = consumer.receive(180, TimeUnit.SECONDS)) != null && numMessage < 3) {
                        numMessage++;
                        consumer.acknowledge(msg);
                    }
                    assertEquals(3, numMessage);
                    assertEquals(0, connectorStatus(ksName, "table1"));
                }
            }
        } finally {
            dumpFunctionLogs("cassandra-source-" + ksName + "-table1");
            undeployConnector(ksName, "table1");
        }
    }

    @Test
    public void testConnectionFailure() throws InterruptedException, IOException {
        final String ksName = "ksx2";
        try (ChaosNetworkContainer<?> chaosContainer = new ChaosNetworkContainer<>(cassandraContainer1.getContainerName(), "100s")) {
            try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS " + ksName +
                        " WITH replication = {'class':'SimpleStrategy','replication_factor':'2'};");
                cqlSession.execute("CREATE TABLE IF NOT EXISTS " + ksName + ".table1 (id text PRIMARY KEY, a int) WITH cdc=true");
                cqlSession.execute("INSERT INTO " + ksName + ".table1 (id, a) VALUES('1',1)");
                cqlSession.execute("INSERT INTO " + ksName + ".table1 (id, a) VALUES('2',1)");
                cqlSession.execute("INSERT INTO " + ksName + ".table1 (id, a) VALUES('3',1)");
            }
            chaosContainer.start();
            deployConnector(ksName, "table1");
            try (PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(pulsarContainer.getPulsarBrokerUrl()).build()) {
                try (Consumer<GenericRecord> consumer = pulsarClient.newConsumer(org.apache.pulsar.client.api.Schema.AUTO_CONSUME())
                        .topic(String.format(Locale.ROOT, "data-%s.table1", ksName))
                        .subscriptionName("sub1")
                        .subscriptionType(SubscriptionType.Key_Shared)
                        .subscriptionMode(SubscriptionMode.Durable)
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                        .subscribe()) {
                    Message<GenericRecord> msg;
                    int numMessage = 0;
                    while ((msg = consumer.receive(240, TimeUnit.SECONDS)) != null && numMessage < 3) {
                        numMessage++;
                        consumer.acknowledge(msg);
                    }
                    assertEquals(3, numMessage);
                    assertEquals(0, connectorStatus(ksName, "table1"));
                }
            }
        } finally {
            dumpFunctionLogs("cassandra-source-" + ksName + "-table1");
            undeployConnector(ksName, "table1");
        }
    }

    // docker exec -it pulsar cat /pulsar/logs/functions/public/default/cassandra-source-ks4-table3/cassandra-source-ks1-table4-0.log
    public void testStaticColumn(String ksName) throws InterruptedException, IOException {
        try {
            try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS " + ksName + " WITH replication = {'class':'SimpleStrategy','replication_factor':'1'};");
                cqlSession.execute("CREATE TABLE IF NOT EXISTS " + ksName + ".table4 (a text, b text, c text, d text static, PRIMARY KEY ((a), b)) with cdc=true;");
                cqlSession.execute("INSERT INTO " + ksName + ".table4 (a,b,c,d) VALUES ('a','b','c','d1');");
            }
            deployConnector(ksName, "table4");

            try (PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(pulsarContainer.getPulsarBrokerUrl()).build()) {
                // test support for static column update in table4
                try (Consumer<GenericRecord> consumer = pulsarClient.newConsumer(org.apache.pulsar.client.api.Schema.AUTO_CONSUME())
                        .topic(String.format(Locale.ROOT, "data-%s.table4", ksName))
                        .subscriptionName("sub1")
                        .subscriptionType(SubscriptionType.Key_Shared)
                        .subscriptionMode(SubscriptionMode.Durable)
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                        .subscribe()) {

                    Message<GenericRecord> msg = consumer.receive(120, TimeUnit.SECONDS);
                    Assert.assertNotNull("Expecting one message, check the agent log", msg);
                    GenericRecord record = msg.getValue();
                    Object key = getKey(msg);
                    Assert.assertEquals("a", getAndAssertKeyFieldAsString(key, "a"));
                    Assert.assertEquals("b", getAndAssertKeyFieldAsString(key, "b"));
                    GenericRecord val = getValue(record);
                    Assert.assertEquals("c", val.getField("c"));
                    Assert.assertEquals("d1", val.getField("d"));
                    consumer.acknowledgeAsync(msg);

                    try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                        cqlSession.execute("INSERT INTO " + ksName + ".table4 (a,d) VALUES ('a','d2');");
                    }
                    msg = consumer.receive(90, TimeUnit.SECONDS);
                    Assert.assertNotNull("Expecting one message, check the agent log", msg);
                    GenericRecord record2 = msg.getValue();
                    Object key2 = getKey(msg);
                    Assert.assertEquals("a", getAndAssertKeyFieldAsString(key2, "a"));
                    assertKeyFieldIsNull(key2, "b");
                    GenericRecord val2 = getValue(record2);
                    Assert.assertNull(val2.getField("c"));  // regular column not fetched
                    Assert.assertEquals("d2", val2.getField("d")); // update static column
                    consumer.acknowledgeAsync(msg);

                    try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                        cqlSession.execute("DELETE FROM " + ksName + ".table4 WHERE a = 'a'");
                    }
                    msg = consumer.receive(90, TimeUnit.SECONDS);
                    Assert.assertNotNull("Expecting one message, check the agent log", msg);
                    GenericRecord record3 = msg.getValue();
                    Object key3 = getKey(msg);
                    GenericRecord val3 = getValue(record3);
                    Assert.assertEquals("a", getAndAssertKeyFieldAsString(key3, "a"));
                    assertKeyFieldIsNull(key3, "b");
                    assertNullValue(val3);
                    consumer.acknowledgeAsync(msg);
                }
            }
        } finally {
            dumpFunctionLogs("cassandra-source-" + ksName + "-table4");
            undeployConnector(ksName, "table4");
        }
    }

    static String genericRecordToString(GenericRecord genericRecord) {
        StringBuilder sb = new StringBuilder("{");
        for(Field field : genericRecord.getFields()) {
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

    Map<String, Object> keyToMap(Object key) {
        if (key instanceof GenericRecord) {
            return genericRecordToMap((GenericRecord) key);
        } else if (key instanceof JsonNode) {
            return jsonNodeToMap((JsonNode) key);
        }

        throw new RuntimeException("unknown key type " + key.getClass().getName());
    }

     Map<String, Object> genericRecordToMap(GenericRecord genericRecord) {
        Map<String, Object> map = new HashMap<>();
        if (outputFormat.contains("json")) {
            return jsonNodeToMap((JsonNode) genericRecord.getNativeObject());
        } else {
            for (Field field : genericRecord.getFields()) {
                map.put(field.getName(), genericRecord.getField(field));
            }
        }

        return map;
    }

    static Map<String, Object> jsonNodeToMap(JsonNode jsonNode) {
        Map<String, Object> map = new HashMap<>();
        for (Iterator<String> it = jsonNode.fieldNames(); it.hasNext(); ) {
            String field = it.next();
            map.put(field, jsonNode.get(field));
        }
        return map;
    }

    private Object getKey(Message<GenericRecord> msg) {
        Object nativeObject = msg.getValue().getNativeObject();
        return (nativeObject instanceof KeyValue) ?
                ((KeyValue<GenericRecord, GenericRecord>)nativeObject).getKey():
                readTree(msg.getKey());
    }

    @SneakyThrows
    private JsonNode readTree(String json)  {
        return mapper.readTree(json);
    }

    private int getAndAssertKeyFieldAsInt(Object key, String fieldName) {
        if (key instanceof GenericRecord) {
            assertTrue(((GenericRecord) key).getField(fieldName) instanceof Integer);
            return  (int)((GenericRecord) key).getField(fieldName);
        } else if (key instanceof JsonNode) {
            assertTrue(((JsonNode) key).get(fieldName).isInt());
            return ((JsonNode) key).get(fieldName).asInt();
        }

        throw new RuntimeException("unknown key type " + key.getClass().getName());
    }

    private String getAndAssertKeyFieldAsString(Object key, String fieldName) {
        if (key instanceof GenericRecord) {
            assertTrue(((GenericRecord) key).getField(fieldName) instanceof String);
            return (String)((GenericRecord) key).getField(fieldName);
        } else if (key instanceof JsonNode) {
            assertTrue(((JsonNode) key).get(fieldName).isTextual());
            return ((JsonNode) key).get(fieldName).asText();
        }

        throw new RuntimeException("unknown key type " + key.getClass().getName());
    }

    private void assertKeyFieldIsNull(Object key, String fieldName) {
        if (key instanceof GenericRecord) {
            assertNull(((GenericRecord) key).getField(fieldName));
        } else if (key instanceof JsonNode) {
            assertTrue(((JsonNode) key).get(fieldName).isNull());
        } else {
            throw new RuntimeException("unknown key type " + key.getClass().getName());
        }
    }

    private List<String> getKeyFields(Object key) {
        if (key instanceof GenericRecord) {
            return ((GenericRecord) key).getFields().stream().map(f->f.getName()).collect(Collectors.toList());
        } else if (key instanceof JsonNode) {
            return Lists.newArrayList(((JsonNode) key).fieldNames());
        }

        throw new RuntimeException("unknown key type " + key.getClass().getName());
    }

    private GenericRecord getValue(GenericRecord genericRecord) {
        return (genericRecord.getNativeObject() instanceof KeyValue) ?
                ((KeyValue<GenericRecord, GenericRecord>)genericRecord.getNativeObject()).getValue() :
                genericRecord;
    }

    private void assertNullValue(GenericRecord value) {
        if (this.outputFormat.equals("json")) {
            // With JSON only format, the data topic receives an empty JSON for delete mutations
            assertEquals("{}", value.getNativeObject().toString());
        } else {
            assertNull(value);
        }
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
        public static final Schema varintType  = CQL_VARINT_LOGICAL_TYPE.addToSchema(Schema.create(Schema.Type.BYTES));

        public static final String CQL_DECIMAL = "cql_decimal";
        public static final String CQL_DECIMAL_BIGINT = "bigint";
        public static final String CQL_DECIMAL_SCALE = "scale";
        public static final CqlDecimalLogicalType CQL_DECIMAL_LOGICAL_TYPE = new CqlDecimalLogicalType();
        public static final Schema decimalType  = CQL_DECIMAL_LOGICAL_TYPE.addToSchema(
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
        public static final Schema durationType  = CQL_DURATION_LOGICAL_TYPE.addToSchema(
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
                return CqlDuration.newInstance((int)value.get(0), (int)value.get(1), (long)value.get(2));
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
