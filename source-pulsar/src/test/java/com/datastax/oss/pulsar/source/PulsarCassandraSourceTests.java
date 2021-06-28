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
import com.datastax.oss.pulsar.source.converters.AvroConverter;
import com.datastax.oss.pulsar.source.converters.JsonConverter;
import com.datastax.testcontainers.cassandra.CassandraContainer;
import com.datastax.testcontainers.pulsar.PulsarContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
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

    @BeforeAll
    public static final void initBeforeClass() throws Exception {
        testNetwork = Network.newNetwork();

        String sourceBuildDir = System.getProperty("sourceBuildDir");
        String projectVersion = System.getProperty("projectVersion");
        String sourceJarFile = String.format(Locale.ROOT, "source-pulsar-%s.nar", projectVersion);
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
    }

    @AfterAll
    public static void closeAfterAll() {
        cassandraContainer1.close();
        cassandraContainer2.close();
        pulsarContainer.close();
    }

    @Test
    public void testWithAvroConverter() throws InterruptedException, IOException {
        testSourceConnector("ks1", AvroConverter.class, AvroConverter.class);
    }

    @Test
    public void testWithJsonConverter() throws InterruptedException, IOException {
        testSourceConnector("ks2", AvroConverter.class, JsonConverter.class);
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

    // docker exec -it pulsar cat /pulsar/logs/functions/public/default/cassandra-source-ks1-table1/cassandra-source-ks1-table1-0.log
    public void testSourceConnector(String ksName,
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

                cqlSession.execute("CREATE TABLE IF NOT EXISTS " + ksName + ".table2 (a text, b int, c int, PRIMARY KEY(a,b)) WITH cdc=true");
                cqlSession.execute("INSERT INTO " + ksName + ".table2 (a,b,c) VALUES('1',1,1)");
                cqlSession.execute("INSERT INTO " + ksName + ".table2 (a,b,c) VALUES('2',1,1)");
                cqlSession.execute("INSERT INTO " + ksName + ".table2 (a,b,c) VALUES('3',1,1)");
            }
            deployConnector(ksName, "table1", keyConverter, valueConverter);
            deployConnector(ksName, "table2", keyConverter, valueConverter);

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
                    while ((msg = consumer.receive(60, TimeUnit.SECONDS)) != null &&
                            mutationTable1.values().stream().mapToInt(i -> i).sum() < 4) {
                        GenericObject genericObject = msg.getValue();
                        assertEquals(SchemaType.KEY_VALUE, genericObject.getSchemaType());
                        KeyValue<GenericRecord, GenericRecord> kv = (KeyValue<GenericRecord, GenericRecord>) genericObject.getNativeObject();
                        GenericRecord key = kv.getKey();
                        GenericRecord value = kv.getValue();
                        System.out.println("Consumer Record: topicName=" + msg.getTopicName() +
                                " key=" + genericRecordToString(key) +
                                " value=" + genericRecordToString(value));
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
                    Thread.sleep(15000);  // wait commitlogs sync on disk
                    while ((msg = consumer.receive(60, TimeUnit.SECONDS)) != null &&
                            mutationTable1.values().stream().mapToInt(i -> i).sum() < 5) {
                        GenericObject genericObject = msg.getValue();
                        assertEquals(SchemaType.KEY_VALUE, genericObject.getSchemaType());
                        KeyValue<GenericRecord, GenericRecord> kv = (KeyValue<GenericRecord, GenericRecord>) genericObject.getNativeObject();
                        GenericRecord key = kv.getKey();
                        GenericRecord value = kv.getValue();
                        System.out.println("Consumer Record: topicName=" + msg.getTopicName() +
                                " key=" + genericRecordToString(key) +
                                " value=" + genericRecordToString(value));
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
                    Thread.sleep(11000);    // wait commitlogs sync on disk
                    while ((msg = consumer.receive(60, TimeUnit.SECONDS)) != null &&
                            mutationTable1.values().stream().mapToInt(i -> i).sum() < 6) {
                        GenericObject genericObject = msg.getValue();
                        assertEquals(SchemaType.KEY_VALUE, genericObject.getSchemaType());
                        KeyValue<GenericRecord, GenericRecord> kv = (KeyValue<GenericRecord, GenericRecord>) genericObject.getNativeObject();
                        GenericRecord key = kv.getKey();
                        GenericRecord value = kv.getValue();
                        System.out.println("Consumer Record: topicName=" + msg.getTopicName() +
                                " key=" + genericRecordToString(key) +
                                " value=" + value);
                        assertEquals("1", key.getField("id"));
                        assertNull(value);
                        mutationTable1.compute((String) key.getField("id"), (k, v) -> v + 1);
                        consumer.acknowledge(msg);
                    }
                    assertEquals((Integer) 3, mutationTable1.get("1"));
                    assertEquals((Integer) 1, mutationTable1.get("2"));
                    assertEquals((Integer) 1, mutationTable1.get("3"));
                }

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
                        System.out.println("Consumer Record: topicName=" + msg.getTopicName() +
                                " key=" + genericRecordToString(key) +
                                " value=" + genericRecordToString(value));
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
                    // TODO: only work for AVO supported types, properly decoded in JSON.
                    try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                        cqlSession.execute("CREATE TYPE " + ksName + ".type2 (a2 int, b2 boolean);");
                        cqlSession.execute("ALTER TABLE " + ksName + ".table2 ADD d type2");
                        cqlSession.execute("INSERT INTO " + ksName + ".table2 (a,b,c,d) VALUES('1',1,1,{a2:1,b2:true})");
                    }
                    Thread.sleep(15000);    // wait commitlogs sync on disk
                    while ((msg = consumer.receive(30, TimeUnit.SECONDS)) != null &&
                            mutationTable2.values().stream().mapToInt(i -> i).sum() < 5) {
                        GenericObject genericObject = msg.getValue();
                        assertEquals(SchemaType.KEY_VALUE, genericObject.getSchemaType());
                        KeyValue<GenericRecord, GenericRecord> kv = (KeyValue<GenericRecord, GenericRecord>) genericObject.getNativeObject();
                        GenericRecord key = kv.getKey();
                        GenericRecord value = kv.getValue();
                        System.out.println("Consumer Record: topicName=" + msg.getTopicName() +
                                " key=" + genericRecordToString(key) +
                                " value=" + genericRecordToString(value));
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
                    Thread.sleep(11000);    // wait commitlogs sync on disk
                    while ((msg = consumer.receive(30, TimeUnit.SECONDS)) != null &&
                            mutationTable2.values().stream().mapToInt(i -> i).sum() < 6) {
                        GenericObject genericObject = msg.getValue();
                        assertEquals(SchemaType.KEY_VALUE, genericObject.getSchemaType());
                        KeyValue<GenericRecord, GenericRecord> kv = (KeyValue<GenericRecord, GenericRecord>) genericObject.getNativeObject();
                        GenericRecord key = kv.getKey();
                        GenericRecord value = kv.getValue();
                        System.out.println("Consumer Record: topicName=" + msg.getTopicName() +
                                " key=" + genericRecordToString(key) +
                                " value=" + value);
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
            dumpFunctionLogs("cassandra-source-ks1-table1");
            dumpFunctionLogs("cassandra-source-ks1-table2");
        }

        undeployConnector(ksName, "table1");
        undeployConnector(ksName, "table2");
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
