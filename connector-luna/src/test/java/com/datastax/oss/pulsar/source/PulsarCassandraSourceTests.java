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
import com.datastax.oss.cdc.CqlLogicalTypes;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.pulsar.source.converters.NativeAvroConverter;
import com.datastax.testcontainers.ChaosNetworkContainer;
import com.datastax.testcontainers.PulsarContainer;
import com.datastax.testcontainers.cassandra.CassandraContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaType;
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
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static com.datastax.oss.cdc.DataSpec.dataSpecMap;

@Slf4j
public class PulsarCassandraSourceTests {
    public static final DockerImageName CASSANDRA_IMAGE = DockerImageName.parse(
            Optional.ofNullable(System.getenv("CASSANDRA_IMAGE"))
                    .orElse("cassandra:" + System.getProperty("cassandraVersion"))
    ).asCompatibleSubstituteFor("cassandra");

    public static final DockerImageName PULSAR_IMAGE = DockerImageName.parse(
            Optional.ofNullable(System.getenv("PULSAR_IMAGE"))
                    .orElse("datastax/lunastreaming:" + System.getProperty("lunaTag"))
    ).asCompatibleSubstituteFor("pulsar");

    private static Network testNetwork;
    private static PulsarContainer<?> pulsarContainer;
    private static CassandraContainer<?> cassandraContainer1;
    private static CassandraContainer<?> cassandraContainer2;

    @BeforeAll
    public static void initBeforeClass() throws Exception {
        testNetwork = Network.newNetwork();

        String connectorBuildDir = System.getProperty("connectorBuildDir");
        String projectVersion = System.getProperty("projectVersion");
        String connectorJarFile = String.format(Locale.ROOT,  "%s-cassandra-source-%s.nar", System.getProperty("pulsarDistribution"), projectVersion);
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
                CASSANDRA_IMAGE, testNetwork, 1, agentBuildDir, "agent-c4-" + System.getProperty("pulsarDistribution"),
                "pulsarServiceUrl=" + pulsarServiceUrl, "c4");
        cassandraContainer2 = CassandraContainer.createCassandraContainerWithAgent(
                CASSANDRA_IMAGE, testNetwork, 2, agentBuildDir, "agent-c4-" + System.getProperty("pulsarDistribution"),
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
    public void testSinglePkWithNativeAvroConverter() throws InterruptedException, IOException {
        testSinglePk("ks1", NativeAvroConverter.class, NativeAvroConverter.class);
    }

    @Test
    public void testCompoundPkWithNativeAvroConverter() throws InterruptedException, IOException {
        testCompoundPk("ks1", null, null);
    }

    @Test
    public void testSchemaWithNativeAvroConverter() throws InterruptedException, IOException {
        testSchema("ks1", NativeAvroConverter.class, NativeAvroConverter.class);
    }

    @Test
    public void testStaticColumnWithNativeAvroConverter() throws InterruptedException, IOException {
        testStaticColumn("ks1", NativeAvroConverter.class, NativeAvroConverter.class);
    }

    @Test
    public void testBatchInsertWithNativeAvroConverter() throws InterruptedException, IOException {
        testBatchInsert("batchinsert", NativeAvroConverter.class, NativeAvroConverter.class);
    }

    void deployConnector(String ksName, String tableName,
                         Class<? extends Converter> keyConverter,
                         Class<? extends Converter> valueConverter) throws IOException, InterruptedException {
        String config = String.format(Locale.ROOT, "{\"%s\":\"%s\", \"%s\":\"%s\", \"%s\":\"%s\", \"%s\":\"%s\", \"%s\": \"%s\", \"%s\":\"%s\" %s %s }",
                CassandraSourceConnectorConfig.CONTACT_POINTS_OPT, "cassandra-1",
                CassandraSourceConnectorConfig.DC_OPT, "datacenter1",
                CassandraSourceConnectorConfig.KEYSPACE_NAME_CONFIG, ksName,
                CassandraSourceConnectorConfig.TABLE_NAME_CONFIG, tableName,
                CassandraSourceConnectorConfig.EVENTS_TOPIC_NAME_CONFIG, "persistent://public/default/events-" + ksName + "." + tableName,
                CassandraSourceConnectorConfig.EVENTS_SUBSCRIPTION_NAME_CONFIG, "sub1",
                keyConverter == null ? "" : ",\"" + CassandraSourceConnectorConfig.KEY_CONVERTER_CLASS_CONFIG + "\":\"" + keyConverter.getName() + "\"",
                valueConverter == null ? "" : ",\"" + CassandraSourceConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG + "\":\"" + valueConverter.getName() + "\"");
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
    public void testSinglePk(String ksName,
                             Class<? extends Converter> keyConverter,
                             Class<? extends Converter> valueConverter) throws InterruptedException, IOException {
        try {
            try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS " + ksName +
                        " WITH replication = {'class':'SimpleStrategy','replication_factor':'2'};");
                cqlSession.execute("CREATE TABLE IF NOT EXISTS " + ksName + ".table1 (id text PRIMARY KEY, a int) WITH cdc=true");
                cqlSession.execute("INSERT INTO " + ksName + ".table1 (id, a) VALUES('1',1)");
                cqlSession.execute("INSERT INTO " + ksName + ".table1 (id, a) VALUES('2',1)");
                cqlSession.execute("INSERT INTO " + ksName + ".table1 (id, a) VALUES('3',1)");
            }
            deployConnector(ksName, "table1", keyConverter, valueConverter);

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
                        GenericObject genericObject = msg.getValue();
                        assertEquals(SchemaType.KEY_VALUE, genericObject.getSchemaType());
                        KeyValue<GenericRecord, GenericRecord> kv = (KeyValue<GenericRecord, GenericRecord>) genericObject.getNativeObject();
                        GenericRecord key = kv.getKey();
                        GenericRecord value = kv.getValue();
                        assertEquals((Integer) 0, mutationTable1.computeIfAbsent((String) key.getField("id"), k -> 0));
                        assertEquals(1, value.getField("a"));
                        mutationTable1.compute((String) key.getField("id"), (k, v) -> v + 1);
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
                        GenericObject genericObject = msg.getValue();
                        assertEquals(SchemaType.KEY_VALUE, genericObject.getSchemaType());
                        KeyValue<GenericRecord, GenericRecord> kv = (KeyValue<GenericRecord, GenericRecord>) genericObject.getNativeObject();
                        GenericRecord key = kv.getKey();
                        GenericRecord value = kv.getValue();
                        assertEquals("1", key.getField("id"));
                        assertEquals(1, value.getField("a"));
                        assertEquals(1.0D, value.getField("b"));
                        mutationTable1.compute((String) key.getField("id"), (k, v) -> v + 1);
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
                        GenericObject genericObject = msg.getValue();
                        assertEquals(SchemaType.KEY_VALUE, genericObject.getSchemaType());
                        KeyValue<GenericRecord, GenericRecord> kv = (KeyValue<GenericRecord, GenericRecord>) genericObject.getNativeObject();
                        GenericRecord key = kv.getKey();
                        GenericRecord value = kv.getValue();
                        assertEquals("1", key.getField("id"));
                        assertNull(value);
                        mutationTable1.compute((String) key.getField("id"), (k, v) -> v + 1);
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
    public void testCompoundPk(String ksName,
                               Class<? extends Converter> keyConverter,
                               Class<? extends Converter> valueConverter) throws InterruptedException, IOException {
        try {
            try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS " + ksName +
                        " WITH replication = {'class':'SimpleStrategy','replication_factor':'2'};");
                cqlSession.execute("CREATE TABLE IF NOT EXISTS " + ksName + ".table2 (a text, b int, c int, PRIMARY KEY(a,b)) WITH cdc=true");
                cqlSession.execute("INSERT INTO " + ksName + ".table2 (a,b,c) VALUES('1',1,1)");
                cqlSession.execute("INSERT INTO " + ksName + ".table2 (a,b,c) VALUES('2',1,1)");
                cqlSession.execute("INSERT INTO " + ksName + ".table2 (a,b,c) VALUES('3',1,1)");
            }
            deployConnector(ksName, "table2", keyConverter, valueConverter);
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
                        GenericObject genericObject = msg.getValue();
                        assertEquals(SchemaType.KEY_VALUE, genericObject.getSchemaType());
                        KeyValue<GenericRecord, GenericRecord> kv = (KeyValue<GenericRecord, GenericRecord>) genericObject.getNativeObject();
                        GenericRecord key = kv.getKey();
                        GenericRecord value = kv.getValue();
                        assertEquals((Integer) 0, mutationTable2.computeIfAbsent((String) key.getField("a"), k -> 0));
                        assertEquals(1, key.getField("b"));
                        assertEquals(1, value.getField("c"));
                        mutationTable2.compute((String) key.getField("a"), (k, v) -> v + 1);
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
                        GenericObject genericObject = msg.getValue();
                        assertEquals(SchemaType.KEY_VALUE, genericObject.getSchemaType());
                        KeyValue<GenericRecord, GenericRecord> kv = (KeyValue<GenericRecord, GenericRecord>) genericObject.getNativeObject();
                        GenericRecord key = kv.getKey();
                        GenericRecord value = kv.getValue();
                        assertEquals("1", key.getField("a"));
                        assertEquals(1, key.getField("b"));
                        assertEquals(1, value.getField("c"));
                        GenericRecord udtGenericRecord = (GenericRecord) value.getField("d");
                        assertEquals(1, udtGenericRecord.getField("a2"));
                        assertEquals(true, udtGenericRecord.getField("b2"));
                        mutationTable2.compute((String) key.getField("a"), (k, v) -> v + 1);
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
                        GenericObject genericObject = msg.getValue();
                        assertEquals(SchemaType.KEY_VALUE, genericObject.getSchemaType());
                        KeyValue<GenericRecord, GenericRecord> kv = (KeyValue<GenericRecord, GenericRecord>) genericObject.getNativeObject();
                        GenericRecord key = kv.getKey();
                        GenericRecord value = kv.getValue();
                        assertEquals("1", key.getField("a"));
                        assertEquals(1, key.getField("b"));
                        assertNull(value);
                        mutationTable2.compute((String) key.getField("a"), (k, v) -> v + 1);
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
    public void testSchema(String ksName,
                           Class<? extends Converter> keyConverter,
                           Class<? extends Converter> valueConverter) throws InterruptedException, IOException {
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

                cqlSession.execute("CREATE TABLE IF NOT EXISTS " + ksName + ".table3 (" +
                        "xtext text, xascii ascii, xboolean boolean, xblob blob, xtimestamp timestamp, xtime time, xdate date, xuuid uuid, xtimeuuid timeuuid, xtinyint tinyint, xsmallint smallint, xint int, xbigint bigint, xvarint varint, xdecimal decimal, xdouble double, xfloat float, xinet4 inet, xinet6 inet, " +
                        "ytext text, yascii ascii, yboolean boolean, yblob blob, ytimestamp timestamp, ytime time, ydate date, yuuid uuid, ytimeuuid timeuuid, ytinyint tinyint, ysmallint smallint, yint int, ybigint bigint, yvarint varint, ydecimal decimal, ydouble double, yfloat float, yinet4 inet, yinet6 inet, yduration duration, yudt zudt, ylist list<text>, yset set<int>, ymap map<text, double>, ylistofmap list<frozen<map<text,double>>>, ysetofudt set<frozen<zudt>>," +
                        "primary key (xtext, xascii, xboolean, xblob, xtimestamp, xtime, xdate, xuuid, xtimeuuid, xtinyint, xsmallint, xint, xbigint, xvarint, xdecimal, xdouble, xfloat, xinet4, xinet6)) " +
                        "WITH CLUSTERING ORDER BY (xascii ASC, xboolean DESC, xblob ASC, xtimestamp DESC, xtime DESC, xdate ASC, xuuid DESC, xtimeuuid ASC, xtinyint DESC, xsmallint ASC, xint DESC, xbigint ASC, xvarint DESC, xdecimal ASC, xdouble DESC, xfloat ASC, xinet4 ASC, xinet6 DESC) AND cdc=true");
                cqlSession.execute("INSERT INTO " + ksName + ".table3 (" +
                                "xtext, xascii, xboolean, xblob, xtimestamp, xtime, xdate, xuuid, xtimeuuid, xtinyint, xsmallint, xint, xbigint, xvarint, xdecimal, xdouble, xfloat, xinet4, xinet6, " +
                                "ytext, yascii, yboolean, yblob, ytimestamp, ytime, ydate, yuuid, ytimeuuid, ytinyint, ysmallint, yint, ybigint, yvarint, ydecimal, ydouble, yfloat, yinet4, yinet6, yduration, yudt, ylist, yset, ymap, ylistofmap, ysetofudt" +
                                ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, ?,?, ?,?,?,?,?)",
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

                        dataSpecMap.get("list").cqlValue,
                        dataSpecMap.get("set").cqlValue,
                        dataSpecMap.get("map").cqlValue,
                        dataSpecMap.get("listofmap").cqlValue,
                        ImmutableSet.of(zudtValue, zudtValue)
                );
            }
            deployConnector(ksName, "table3", keyConverter, valueConverter);
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
                        GenericObject genericObject = msg.getValue();
                        mutationTable3Count++;
                        assertEquals(SchemaType.KEY_VALUE, genericObject.getSchemaType());
                        KeyValue<GenericRecord, GenericRecord> kv = (KeyValue<GenericRecord, GenericRecord>) genericObject.getNativeObject();
                        GenericRecord key = kv.getKey();
                        GenericRecord value = kv.getValue();

                        // check primary key fields
                        Map<String, Object> keyMap = genericRecordToMap(key);
                        for (Field field : key.getFields()) {
                            assertField(field.getName(), keyMap.get(field.getName()));
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

    void assertGenericMap(String field, Map<org.apache.avro.util.Utf8, Object> gm) {
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

    void assertGenericArray(String field, org.apache.avro.generic.GenericArray ga) {
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
                    Map<org.apache.avro.util.Utf8, Object> gm = (Map<org.apache.avro.util.Utf8, Object>) ga.get(i);
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
                    org.apache.avro.generic.GenericData.Record gr = (org.apache.avro.generic.GenericData.Record) ga.get(i);
                    for(org.apache.avro.Schema.Field f : gr.getSchema().getFields()) {
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
        if (!vKey.equals("udt") && ! vKey.equals("setofudt")) {
            Assert.assertTrue("Unknown field " + vKey, dataSpecMap.containsKey(vKey));
        }
        if (value instanceof GenericRecord) {
            assertGenericRecords(vKey, (GenericRecord) value);
        } else if (value instanceof Collection) {
            assertGenericArray(vKey, (org.apache.avro.generic.GenericArray) value);
        } else if (value instanceof Map) {
            assertGenericMap(vKey, (Map) value);
        } else if (value instanceof org.apache.avro.util.Utf8) {
            // convert Utf8 to String
            Assert.assertEquals("Wrong value for regular field " + fieldName, dataSpecMap.get(vKey).avroValue, value.toString());
        } else if (value instanceof org.apache.avro.generic.GenericData.Record) {
            org.apache.avro.generic.GenericData.Record gr = (org.apache.avro.generic.GenericData.Record) value;
            if (CqlLogicalTypes.CQL_DECIMAL.equals(gr.getSchema().getName())) {
                CqlLogicalTypes.CqlDecimalConversion cqlDecimalConversion = new CqlLogicalTypes.CqlDecimalConversion();
                value = cqlDecimalConversion.fromRecord(gr, gr.getSchema(), CqlLogicalTypes.CQL_DECIMAL_LOGICAL_TYPE);
            } else if (CqlLogicalTypes.CQL_VARINT.equals(gr.getSchema().getName())) {
                CqlLogicalTypes.CqlVarintConversion cqlVarintConversion = new CqlLogicalTypes.CqlVarintConversion();
                value = cqlVarintConversion.fromRecord(gr, gr.getSchema(), CqlLogicalTypes.CQL_VARINT_LOGICAL_TYPE);
            } else if (CqlLogicalTypes.CQL_DURATION.equals(gr.getSchema().getName())) {
                NativeAvroConverter.CqlDurationConversion  cqlDurationConversion = new NativeAvroConverter.CqlDurationConversion();
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
        }
        Assert.assertTrue("Unexpected field="+field, false);
    }

    public void testBatchInsert(String ksName,
                             Class<? extends Converter> keyConverter,
                             Class<? extends Converter> valueConverter) throws InterruptedException, IOException {
        try {
            try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS " + ksName +
                        " WITH replication = {'class':'SimpleStrategy','replication_factor':'2'};");
                cqlSession.execute("CREATE TABLE IF NOT EXISTS " + ksName + ".table1 (id text, a int, b int, PRIMARY KEY (id, a)) WITH cdc=true");
            }
            deployConnector(ksName, "table1", keyConverter, valueConverter);

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
                        GenericObject genericObject = msg.getValue();
                        assertEquals(SchemaType.KEY_VALUE, genericObject.getSchemaType());
                        KeyValue<GenericRecord, GenericRecord> kv = (KeyValue<GenericRecord, GenericRecord>) genericObject.getNativeObject();
                        GenericRecord key = kv.getKey();
                        Assert.assertTrue(((String)key.getField("id")).startsWith("a"));
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

                deployConnector(ksName, "table1", NativeAvroConverter.class, NativeAvroConverter.class);

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
        try(ChaosNetworkContainer<?> chaosContainer = new ChaosNetworkContainer<>(cassandraContainer1.getContainerName(), "100s")) {
            try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS " + ksName +
                        " WITH replication = {'class':'SimpleStrategy','replication_factor':'2'};");
                cqlSession.execute("CREATE TABLE IF NOT EXISTS " + ksName + ".table1 (id text PRIMARY KEY, a int) WITH cdc=true");
                cqlSession.execute("INSERT INTO " + ksName + ".table1 (id, a) VALUES('1',1)");
                cqlSession.execute("INSERT INTO " + ksName + ".table1 (id, a) VALUES('2',1)");
                cqlSession.execute("INSERT INTO " + ksName + ".table1 (id, a) VALUES('3',1)");
            }
            chaosContainer.start();
            deployConnector(ksName, "table1", NativeAvroConverter.class, NativeAvroConverter.class);
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

    // docker exec -it pulsar cat /pulsar/logs/functions/public/default/cassandra-source-ks4-table3/cassandra-source-ks1-table4-0.log
    public void testStaticColumn(String ksName,
                                 Class<? extends Converter> keyConverter,
                                 Class<? extends Converter> valueConverter) throws InterruptedException, IOException {
        try {
            try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS " + ksName + " WITH replication = {'class':'SimpleStrategy','replication_factor':'1'};");
                cqlSession.execute("CREATE TABLE IF NOT EXISTS " + ksName + ".table4 (a text, b text, c text, d text static, PRIMARY KEY ((a), b)) with cdc=true;");
                cqlSession.execute("INSERT INTO " + ksName + ".table4 (a,b,c,d) VALUES ('a','b','c','d1');");
            }
            deployConnector(ksName, "table4", keyConverter, valueConverter);

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
                    GenericRecord gr = msg.getValue();
                    KeyValue<GenericRecord, GenericRecord> kv = (KeyValue<GenericRecord, GenericRecord>) gr.getNativeObject();
                    GenericRecord key = kv.getKey();
                    Assert.assertEquals("a", key.getField("a"));
                    Assert.assertEquals("b", key.getField("b"));
                    GenericRecord val = kv.getValue();
                    Assert.assertEquals("c", val.getField("c"));
                    Assert.assertEquals("d1", val.getField("d"));
                    consumer.acknowledgeAsync(msg);

                    try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                        cqlSession.execute("INSERT INTO " + ksName + ".table4 (a,d) VALUES ('a','d2');");
                    }
                    msg = consumer.receive(90, TimeUnit.SECONDS);
                    Assert.assertNotNull("Expecting one message, check the agent log", msg);
                    GenericRecord gr2 = msg.getValue();
                    KeyValue<GenericRecord, GenericRecord> kv2 = (KeyValue<GenericRecord, GenericRecord>) gr2.getNativeObject();
                    GenericRecord key2 = kv2.getKey();
                    Assert.assertEquals("a", key2.getField("a"));
                    Assert.assertEquals(null, key2.getField("b"));
                    GenericRecord val2 = kv2.getValue();
                    Assert.assertNull(val2.getField("c"));  // regular column not fetched
                    Assert.assertEquals("d2", val2.getField("d")); // update static column
                    consumer.acknowledgeAsync(msg);

                    try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                        cqlSession.execute("DELETE FROM " + ksName + ".table4 WHERE a = 'a'");
                    }
                    msg = consumer.receive(90, TimeUnit.SECONDS);
                    Assert.assertNotNull("Expecting one message, check the agent log", msg);
                    GenericRecord gr3 = msg.getValue();
                    KeyValue<GenericRecord, GenericRecord> kv3 = (KeyValue<GenericRecord, GenericRecord>) gr3.getNativeObject();
                    GenericRecord key3 = kv3.getKey();
                    Assert.assertEquals("a", key3.getField("a"));
                    Assert.assertEquals(null, key3.getField("b"));
                    Assert.assertNull(kv3.getValue());
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

    static Map<String, Object> genericRecordToMap(GenericRecord genericRecord) {
        Map<String, Object> map = new HashMap<>();
        for (Field field : genericRecord.getFields()) {
            map.put(field.getName(), genericRecord.getField(field));
        }
        return map;
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
}
