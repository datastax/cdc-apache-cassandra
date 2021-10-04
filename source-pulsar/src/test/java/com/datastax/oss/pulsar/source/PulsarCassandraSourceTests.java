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

import com.datastax.cassandra.cdc.CqlLogicalTypes;
import com.datastax.oss.cdc.CassandraSourceConnectorConfig;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.pulsar.source.converters.NativeAvroConverter;
import com.datastax.testcontainers.cassandra.CassandraContainer;
import com.datastax.testcontainers.pulsar.PulsarContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericData;
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
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@Slf4j
public class PulsarCassandraSourceTests {
    public static final DockerImageName CASSANDRA_IMAGE = DockerImageName.parse(
            Optional.ofNullable(System.getenv("CASSANDRA_IMAGE")).orElse("cassandra:4.0-beta4")
    ).asCompatibleSubstituteFor("cassandra");

    public static final DockerImageName PULSAR_IMAGE = DockerImageName.parse(
            Optional.ofNullable(System.getenv("PULSAR_IMAGE")).orElse("harbor.sjc.dsinternal.org/pulsar/lunastreaming:latest-272")
    ).asCompatibleSubstituteFor("pulsar");

    private static Network testNetwork;
    private static PulsarContainer<?> pulsarContainer;
    private static CassandraContainer<?> cassandraContainer1;
    private static CassandraContainer<?> cassandraContainer2;
    private static final Map<String, Object[]> values = new HashMap<>();

    @BeforeAll
    public static final void initBeforeClass() throws Exception {
        testNetwork = Network.newNetwork();

        String sourceBuildDir = System.getProperty("sourceBuildDir");
        String projectVersion = System.getProperty("projectVersion");
        String sourceJarFile = String.format(Locale.ROOT, "pulsar-cassandra-source-%s.nar", projectVersion);
        pulsarContainer = new PulsarContainer<>(PULSAR_IMAGE)
                .withNetwork(testNetwork)
                .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd.withName("pulsar"))
                .withFunctionsWorker()
                .withFileSystemBind(
                        String.format(Locale.ROOT, "%s/libs/%s", sourceBuildDir, sourceJarFile),
                        String.format(Locale.ROOT, "/pulsar/connectors/%s", sourceJarFile))
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
        String producerBuildDir = System.getProperty("producerBuildDir");
        cassandraContainer1 = CassandraContainer.createCassandraContainerWithPulsarProducer(
                CASSANDRA_IMAGE, testNetwork, 1, producerBuildDir,"v4", pulsarServiceUrl);
        cassandraContainer2 = CassandraContainer.createCassandraContainerWithPulsarProducer(
                CASSANDRA_IMAGE, testNetwork, 2, producerBuildDir,"v4", pulsarServiceUrl);
        cassandraContainer1.start();
        cassandraContainer2.start();

        final ZoneId zone = ZoneId.systemDefault();
        final LocalDate localDate = LocalDate.of(2020, 12, 25);
        final LocalDateTime localDateTime = localDate.atTime(10, 10, 00);

        // sample values to check CQL to Pulsar native types, left=CQL value, right=Pulsar value
        values.put("text", new Object[]{"a", "a"});
        values.put("ascii", new Object[] {"aa", "aa"});
        values.put("boolean", new Object[] {true, true});
        values.put("blob", new Object[] {ByteBuffer.wrap(new byte[]{0x00, 0x01}), ByteBuffer.wrap(new byte[]{0x00, 0x01})});
        values.put("timestamp", new Object[] {localDateTime.atZone(zone).toInstant(), localDateTime.atZone(zone).toInstant().toEpochMilli()}); // long milliseconds since epochday
        values.put("time", new Object[] {localDateTime.toLocalTime(), (localDateTime.toLocalTime().toNanoOfDay() / 1000)}); // long microseconds since midnight
        values.put("date", new Object[] {localDateTime.toLocalDate(), (int) localDateTime.toLocalDate().toEpochDay()}); // int seconds since epochday
        values.put("uuid", new Object[] {UUID.fromString("01234567-0123-0123-0123-0123456789ab"), "01234567-0123-0123-0123-0123456789ab"});
        values.put("timeuuid", new Object[] {UUID.fromString("d2177dd0-eaa2-11de-a572-001b779c76e3"), "d2177dd0-eaa2-11de-a572-001b779c76e3"});
        values.put("tinyint", new Object[] {(byte) 0x01, (int) 0x01}); // Avro only support integer
        values.put("smallint", new Object[] {(short) 1, (int) 1});     // Avro only support integer
        values.put("int", new Object[] {1, 1});
        values.put("bigint", new Object[] {1L, 1L});
        values.put("double", new Object[] {1.0D, 1.0D});
        values.put("float", new Object[] {1.0f, 1.0f});
        values.put("inet4", new Object[] {Inet4Address.getLoopbackAddress(), Inet4Address.getLoopbackAddress().getHostAddress()});
        values.put("inet6", new Object[] {Inet6Address.getLoopbackAddress(), Inet6Address.getLoopbackAddress().getHostAddress()});
        values.put("varint", new Object[] {new BigInteger("314"), new CqlLogicalTypes.CqlVarintConversion().toBytes(new BigInteger("314"), CqlLogicalTypes.varintType, CqlLogicalTypes.CQL_VARINT_LOGICAL_TYPE)});
        values.put("decimal", new Object[] {new BigDecimal(314.16), new BigDecimal(314.16)});
        values.put("duration", new Object[] { CqlDuration.newInstance(1,2,3), CqlDuration.newInstance(1,2,3)});
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
        testCompoundPk("ks1", NativeAvroConverter.class, NativeAvroConverter.class);
    }

    @Test
    public void testSchemaWithNativeAvroConverter() throws InterruptedException, IOException {
        testSchema("ks1", NativeAvroConverter.class, NativeAvroConverter.class);
    }

    @Test
    public void testStaticColumnWithNativeAvroConverter() throws InterruptedException, IOException {
        testStaticColumn("ks1", NativeAvroConverter.class, NativeAvroConverter.class);
    }

    void deployConnector(String ksName, String tableName,
                         Class<? extends Converter> keyConverter,
                         Class<? extends Converter> valueConverter) throws IOException, InterruptedException {
        Container.ExecResult result = pulsarContainer.execInContainer(
                "/pulsar/bin/pulsar-admin",
                "source", "create",
                "--source-type", "cassandra-source",
                "--tenant", "public",
                "--namespace", "default",
                "--name", "cassandra-source-" + ksName + "-" + tableName,
                "--destination-topic-name", "data-" + ksName + "." + tableName,
                "--source-config",
                String.format(Locale.ROOT, "{\"%s\":\"%s\", \"%s\":\"%s\", \"%s\":\"%s\", \"%s\":\"%s\", \"%s\": \"%s\", \"%s\":\"%s\", \"%s\":\"%s\", \"%s\":\"%s\"}",
                        CassandraSourceConnectorConfig.CONTACT_POINTS_OPT, "cassandra-1",
                        CassandraSourceConnectorConfig.DC_OPT, "datacenter1",
                        CassandraSourceConnectorConfig.KEYSPACE_NAME_CONFIG, ksName,
                        CassandraSourceConnectorConfig.TABLE_NAME_CONFIG, tableName,
                        CassandraSourceConnectorConfig.EVENTS_TOPIC_NAME_CONFIG, "persistent://public/default/events-" + ksName + "." + tableName,
                        CassandraSourceConnectorConfig.EVENTS_SUBSCRIPTION_NAME_CONFIG, "sub1",
                        CassandraSourceConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, keyConverter.getName(),
                        CassandraSourceConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, valueConverter.getName()));
        assertEquals(0, result.getExitCode());
    }

    void undeployConnector(String ksName, String tableName) throws IOException, InterruptedException {
        Container.ExecResult result = pulsarContainer.execInContainer(
                "/pulsar/bin/pulsar-admin", "source", "delete",
                "--tenant", "public",
                "--namespace", "default",
                "--name", "cassandra-source-" + ksName + "-" + tableName);
        assertEquals(0, result.getExitCode());
    }

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
                        "ztext text, zascii ascii, zboolean boolean, zblob blob, ztimestamp timestamp, ztime time, zdate date, zuuid uuid, ztimeuuid timeuuid, ztinyint tinyint, zsmallint smallint, zint int, zbigint bigint, zvarint varint, zdecimal decimal, zduration duration, zdouble double, zfloat float, zinet4 inet, zinet6 inet" +
                        ");");
                UserDefinedType zudt =
                        cqlSession.getMetadata()
                                .getKeyspace(ksName)
                                .flatMap(ks -> ks.getUserDefinedType("zudt"))
                                .orElseThrow(() -> new IllegalArgumentException("Missing UDT zudt definition"));

                cqlSession.execute("CREATE TABLE IF NOT EXISTS " + ksName + ".table3 (" +
                        "xtext text, xascii ascii, xboolean boolean, xblob blob, xtimestamp timestamp, xtime time, xdate date, xuuid uuid, xtimeuuid timeuuid, xtinyint tinyint, xsmallint smallint, xint int, xbigint bigint, xvarint varint, xdecimal decimal, xdouble double, xfloat float, xinet4 inet, xinet6 inet, " +
                        "ytext text, yascii ascii, yboolean boolean, yblob blob, ytimestamp timestamp, ytime time, ydate date, yuuid uuid, ytimeuuid timeuuid, ytinyint tinyint, ysmallint smallint, yint int, ybigint bigint, yvarint varint, ydecimal decimal, ydouble double, yfloat float, yinet4 inet, yinet6 inet, yduration duration, yudt zudt, " +
                        "primary key (xtext, xascii, xboolean, xblob, xtimestamp, xtime, xdate, xuuid, xtimeuuid, xtinyint, xsmallint, xint, xbigint, xvarint, xdecimal, xdouble, xfloat, xinet4, xinet6)) WITH cdc=true");
                cqlSession.execute("INSERT INTO " + ksName + ".table3 (" +
                                "xtext, xascii, xboolean, xblob, xtimestamp, xtime, xdate, xuuid, xtimeuuid, xtinyint, xsmallint, xint, xbigint, xvarint, xdecimal, xdouble, xfloat, xinet4, xinet6, " +
                                "ytext, yascii, yboolean, yblob, ytimestamp, ytime, ydate, yuuid, ytimeuuid, ytinyint, ysmallint, yint, ybigint, yvarint, ydecimal, ydouble, yfloat, yinet4, yinet6, yduration, yudt" +
                                ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, ?,?)",
                        values.get("text")[0],
                        values.get("ascii")[0],
                        values.get("boolean")[0],
                        values.get("blob")[0],
                        values.get("timestamp")[0],
                        values.get("time")[0],
                        values.get("date")[0],
                        values.get("uuid")[0],
                        values.get("timeuuid")[0],
                        values.get("tinyint")[0],
                        values.get("smallint")[0],
                        values.get("int")[0],
                        values.get("bigint")[0],
                        values.get("varint")[0],
                        values.get("decimal")[0],
                        values.get("double")[0],
                        values.get("float")[0],
                        values.get("inet4")[0],
                        values.get("inet6")[0],

                        values.get("text")[0],
                        values.get("ascii")[0],
                        values.get("boolean")[0],
                        values.get("blob")[0],
                        values.get("timestamp")[0],
                        values.get("time")[0],
                        values.get("date")[0],
                        values.get("uuid")[0],
                        values.get("timeuuid")[0],
                        values.get("tinyint")[0],
                        values.get("smallint")[0],
                        values.get("int")[0],
                        values.get("bigint")[0],
                        values.get("varint")[0],
                        values.get("decimal")[0],
                        values.get("double")[0],
                        values.get("float")[0],
                        values.get("inet4")[0],
                        values.get("inet6")[0],

                        values.get("duration")[0],
                        zudt.newValue(
                                values.get("text")[0],
                                values.get("ascii")[0],
                                values.get("boolean")[0],
                                values.get("blob")[0],
                                values.get("timestamp")[0],
                                values.get("time")[0],
                                values.get("date")[0],
                                values.get("uuid")[0],
                                values.get("timeuuid")[0],
                                values.get("tinyint")[0],
                                values.get("smallint")[0],
                                values.get("int")[0],
                                values.get("bigint")[0],
                                values.get("varint")[0],
                                values.get("decimal")[0],
                                values.get("duration")[0],
                                values.get("double")[0],
                                values.get("float")[0],
                                values.get("inet4")[0],
                                values.get("inet6")[0]
                        )
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
                        Map<String, Object> keyMap = genericRecordToMap(key);
                        Map<String, Object> valueMap = genericRecordToMap(value);
                        // check primary key fields
                        for (Field field : key.getFields()) {
                            String vKey = field.getName().substring(1);
                            Assert.assertTrue("Unknown field " + vKey, values.containsKey(vKey));
                            if (keyMap.get(field.getName()) instanceof GenericRecord) {
                                assertGenericRecords(vKey, (GenericRecord) keyMap.get(field.getName()));
                            } else {
                                Assert.assertEquals("Wrong value for PK field " + field.getName(), values.get(vKey)[1], keyMap.get(field.getName()));
                            }
                        }

                        // check regular columns.
                        for (Field field : value.getFields()) {
                            if (field.getName().equals("yudt"))
                                continue;
                            String vKey = field.getName().substring(1);
                            Assert.assertTrue("Unknown field " + vKey, values.containsKey(vKey));
                            if (valueMap.get(field.getName()) instanceof GenericRecord) {
                                assertGenericRecords(vKey, (GenericRecord) valueMap.get(field.getName()));
                            } else {
                                Assert.assertEquals("Wrong value for regular field " + field.getName(), values.get(vKey)[1], valueMap.get(field.getName()));
                            }
                        }
                        assertGenericRecords("duration", (GenericRecord) valueMap.get("yduration"));

                        // check UDT
                        GenericRecord yudt = (GenericRecord) value.getField("yudt");
                        for (Field field : yudt.getFields()) {
                            String vKey = field.getName().substring(1);
                            Assert.assertTrue("Unknown field " + vKey, values.containsKey(vKey));
                            if (yudt.getField(field.getName()) instanceof GenericRecord) {
                                assertGenericRecords(vKey, (GenericRecord) yudt.getField(field.getName()));
                            } else {
                                Assert.assertEquals("Wrong value for udt field " + field.getName(), values.get(vKey)[1], yudt.getField(field.getName()));
                            }
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

    void assertGenericRecords(String field, GenericRecord gr) {
        switch (field) {
            case "decimal": {
                ByteBuffer bb = (ByteBuffer) gr.getField(CqlLogicalTypes.CQL_DECIMAL_BIGINT);
                byte[] bytes = new byte[bb.remaining()];
                bb.duplicate().get(bytes);
                BigInteger bigInteger = new BigInteger(bytes);
                BigDecimal bigDecimal = new BigDecimal(bigInteger, (int) gr.getField(CqlLogicalTypes.CQL_DECIMAL_SCALE));
                Assert.assertEquals("Wrong value for field " + field, values.get(field)[1], bigDecimal);
            }
            break;
            case "duration": {
                Assert.assertEquals("Wrong value for field " + field, values.get(field)[1],
                        CqlDuration.newInstance(
                                (int) gr.getField(CqlLogicalTypes.CQL_DURATION_MONTHS),
                                (int) gr.getField(CqlLogicalTypes.CQL_DURATION_DAYS),
                                (long) gr.getField(CqlLogicalTypes.CQL_DURATION_NANOSECONDS)));
            }
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
                    Assert.assertNotNull("Expecting one message, check the producer log", msg);
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
                    Assert.assertNotNull("Expecting one message, check the producer log", msg);
                    GenericRecord gr2 = msg.getValue();
                    KeyValue<GenericRecord, GenericRecord> kv2 = (KeyValue<GenericRecord, GenericRecord>) gr2.getNativeObject();
                    GenericRecord key2 = kv2.getKey();
                    Assert.assertEquals("a", key2.getField("a"));
                    Assert.assertEquals(null, key2.getField("b"));
                    GenericRecord val2 = kv2.getValue();
                    Assert.assertEquals("c", val2.getField("c"));
                    Assert.assertEquals("d2", val2.getField("d"));
                    consumer.acknowledgeAsync(msg);

                    try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                        cqlSession.execute("DELETE FROM " + ksName + ".table4 WHERE a = 'a'");
                    }
                    msg = consumer.receive(90, TimeUnit.SECONDS);
                    Assert.assertNotNull("Expecting one message, check the producer log", msg);
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
                return IOUtils.toString(inputStream, "utf-8");
            });
            log.info("Function {} logs {}", name, logs);
        } catch (Throwable err) {
            log.info("Cannot download {} logs", name, err);
        }
    }
}
