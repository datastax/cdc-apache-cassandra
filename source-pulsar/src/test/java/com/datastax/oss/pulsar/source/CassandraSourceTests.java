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
import com.datastax.oss.common.sink.config.ContactPointsValidator;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.pulsar.source.converters.AvroConverter;
import com.datastax.oss.pulsar.source.converters.JsonConverter;
import com.datastax.testcontainers.cassandra.CassandraContainer;
import com.datastax.testcontainers.pulsar.PulsarContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class CassandraSourceTests {
    public static final String CASSANDRA_IMAGE = "cassandra:4.0-beta4";
    public static final String PULSAR_VERSION = "latest";

    static final String PULSAR_IMAGE = "strapdata/pulsar-all:" + PULSAR_VERSION;

    private static Network testNetwork;
    private static PulsarContainer<?> pulsarContainer;
    private static String projectVersion = System.getProperty("projectVersion");

    private static CassandraContainer<?> cassandraContainer;

    @BeforeAll
    public static final void initBeforeClass() throws Exception {
        testNetwork = Network.newNetwork();

        String sourceBuildDir = System.getProperty("sourceBuildDir");
        String projectVersion = System.getProperty("projectVersion");
        String sourceJarFile = String.format(Locale.ROOT, "source-pulsar-%s.nar", projectVersion);
        pulsarContainer = new PulsarContainer<>(DockerImageName.parse(PULSAR_IMAGE))
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

        String producerBuildDir = System.getProperty("producerBuildDir");
        String producerJarFile = String.format(Locale.ROOT, "producer-v4-pulsar-%s-all.jar", projectVersion);
        cassandraContainer = new CassandraContainer<>(CASSANDRA_IMAGE)
                .withCreateContainerCmdModifier(c -> c.withName("cassandra"))
                .withLogConsumer(new Slf4jLogConsumer(log))
                .withNetwork(testNetwork)
                .withConfigurationOverride("cassandra-cdc")
                .withFileSystemBind(
                        String.format(Locale.ROOT, "%s/libs/%s", producerBuildDir, producerJarFile),
                        String.format(Locale.ROOT, "/%s", producerJarFile))
                .withEnv("JVM_EXTRA_OPTS", String.format(
                        Locale.ROOT,
                        "-javaagent:/%s=pulsarServiceUrl=%s",
                        producerJarFile, "pulsar://pulsar:" + pulsarContainer.BROKER_PORT))
                .withStartupTimeout(Duration.ofSeconds(120));
        cassandraContainer.start();
    }

    @AfterAll
    public static void closeAfterAll() {
        pulsarContainer.close();
        cassandraContainer.close();
    }

    @Test
    public void testWithAvroConverter() throws InterruptedException, IOException {
        testSourceConnector("ks1", AvroConverter.class, AvroConverter.class);
    }

    @Test
    public void testWithJsonConverter() throws InterruptedException, IOException {
        testSourceConnector("ks2", AvroConverter.class, JsonConverter.class);
    }

    public void testSourceConnector(String ksName,
                                    Class<? extends Converter> keyConverter,
                                    Class<? extends Converter> valueConverter) throws InterruptedException, IOException {
        Container.ExecResult result = pulsarContainer.execInContainer(
                "/pulsar/bin/pulsar-admin",
                "source", "create",
                "--source-type", "cassandra-source",
                "--tenant", "public",
                "--namespace", "default",
                "--name", "cassandra-source-" + ksName + "-1",
                "--destination-topic-name", "data-" + ksName + ".table1",
                "--source-config",
                String.format(Locale.ROOT, "{\"%s\":\"cassandra\", \"%s\":\"datacenter1\", \"%s\":\"%s\", \"%s\":\"%s\", \"%s\": \"%s\", \"%s\":\"sub1\", \"%s\":\"%s\",\"%s\":\"%s\"}",
                        ContactPointsValidator.CONTACT_POINTS_OPT,
                        CassandraSourceConnectorConfig.DC_OPT,
                        CassandraSourceConnectorConfig.KEYSPACE_NAME_CONFIG, ksName,
                        CassandraSourceConnectorConfig.TABLE_NAME_CONFIG, "table1",
                        CassandraSourceConnectorConfig.EVENTS_TOPIC_NAME_CONFIG, "persistent://public/default/events-" + ksName + ".table1",
                        CassandraSourceConnectorConfig.EVENTS_SUBSCRIPTION_NAME_CONFIG,
                        CassandraSourceConnectorConfig.KEY_CONVERTER_CLASS_CONFIG,
                        keyConverter.getName(),
                        CassandraSourceConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG,
                        valueConverter.getName()));
        assertEquals(0, result.getExitCode());

        try (CqlSession cqlSession = cassandraContainer.getCqlSession()) {
            cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS " + ksName +
                    " WITH replication = {'class':'SimpleStrategy','replication_factor':'1'};");
            cqlSession.execute("CREATE TABLE IF NOT EXISTS " + ksName + ".table1 (id text PRIMARY KEY, a int) WITH cdc=true");
            cqlSession.execute("INSERT INTO " + ksName + ".table1 (id, a) VALUES('1',1)");
            cqlSession.execute("INSERT INTO " + ksName + ".table1 (id, a) VALUES('2',1)");
            cqlSession.execute("INSERT INTO " + ksName + ".table1 (id, a) VALUES('3',1)");

            cqlSession.execute("CREATE TABLE IF NOT EXISTS " + ksName + ".table2 (a text, b int, c int, PRIMARY KEY(a,b)) WITH cdc=true");
            cqlSession.execute("INSERT INTO " + ksName + ".table2 (a,b,c) VALUES('1',1,1)");
            cqlSession.execute("INSERT INTO " + ksName + ".table2 (a,b,c) VALUES('2',1,1)");
            cqlSession.execute("INSERT INTO " + ksName + ".table2 (a,b,c) VALUES('3',1,1)");
        }

        // wait commitlogs sync on disk
        Thread.sleep(11000);

        try (PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(pulsarContainer.getPulsarBrokerUrl()).build()) {
            // pulsar-admin schemas get "persistent://public/default/events-ks1.table1"
            // pulsar-admin topics peek-messages persistent://public/default/events-ks1.table1-partition-0 --count 3 --subscription sub1
            int mutationTable1 = 1;
            try (Consumer<GenericRecord> consumer = pulsarClient.newConsumer(org.apache.pulsar.client.api.Schema.AUTO_CONSUME())
                    .topic(String.format(Locale.ROOT, "data-%s.table1", ksName))
                    .subscriptionName("sub1")
                    .subscriptionType(SubscriptionType.Key_Shared)
                    .subscriptionMode(SubscriptionMode.Durable)
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .subscribe()) {
                Message<GenericRecord> msg;
                while ((msg = consumer.receive(30, TimeUnit.SECONDS)) != null && mutationTable1 < 4) {
                    GenericObject genericObject = msg.getValue();
                    assertEquals(SchemaType.KEY_VALUE, genericObject.getSchemaType());
                    KeyValue<GenericRecord, GenericRecord> kv = (KeyValue<GenericRecord, GenericRecord>) genericObject.getNativeObject();
                    GenericRecord key = kv.getKey();
                    GenericRecord value = kv.getValue();
                    System.out.println("Consumer Record: topicName=" + msg.getTopicName() +
                            " key=" + key +
                            " value=" + value);
                    assertEquals(Integer.toString(mutationTable1), key.getField("id"));
                    assertEquals(1, value.getField("a"));
                    mutationTable1++;
                }
            }
            assertEquals(4, mutationTable1);

            result = pulsarContainer.execInContainer(
                    "/pulsar/bin/pulsar-admin",
                    "source", "create",
                    "--source-type", "cassandra-source",
                    "--tenant", "public",
                    "--namespace", "default",
                    "--name", "cassandra-source-" + ksName + "-2",
                    "--destination-topic-name", "data-" + ksName + ".table2",
                    "--source-config",
                    String.format(Locale.ROOT, "{\"%s\":\"cassandra\", \"%s\":\"datacenter1\", \"%s\":\"%s\", \"%s\":\"%s\", \"%s\": \"%s\", \"%s\":\"sub1\", \"%s\":\"%s\",\"%s\":\"%s\"}",
                            ContactPointsValidator.CONTACT_POINTS_OPT,
                            CassandraSourceConnectorConfig.DC_OPT,
                            CassandraSourceConnectorConfig.KEYSPACE_NAME_CONFIG, ksName,
                            CassandraSourceConnectorConfig.TABLE_NAME_CONFIG, "table2",
                            CassandraSourceConnectorConfig.EVENTS_TOPIC_NAME_CONFIG, "persistent://public/default/events-" + ksName + ".table2",
                            CassandraSourceConnectorConfig.EVENTS_SUBSCRIPTION_NAME_CONFIG,
                            CassandraSourceConnectorConfig.KEY_CONVERTER_CLASS_CONFIG,
                            keyConverter.getName(),
                            CassandraSourceConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG,
                            valueConverter.getName()));
            assertEquals(0, result.getExitCode());

            int mutationTable2 = 1;
            try (Consumer<GenericRecord> consumer = pulsarClient.newConsumer(org.apache.pulsar.client.api.Schema.AUTO_CONSUME())
                    .topic(String.format(Locale.ROOT, "data-%s.table2", ksName))
                    .subscriptionName("sub1")
                    .subscriptionType(SubscriptionType.Key_Shared)
                    .subscriptionMode(SubscriptionMode.Durable)
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .subscribe()) {
                Message<GenericRecord> msg;
                while ((msg = consumer.receive(30, TimeUnit.SECONDS)) != null && mutationTable2 < 4) {
                    GenericObject genericObject = msg.getValue();
                    assertEquals(SchemaType.KEY_VALUE, genericObject.getSchemaType());
                    KeyValue<GenericRecord, GenericRecord> kv = (KeyValue<GenericRecord, GenericRecord>) genericObject.getNativeObject();
                    GenericRecord key = kv.getKey();
                    GenericRecord value = kv.getValue();
                    System.out.println("Consumer Record: topicName=" + msg.getTopicName() +
                            " key=" + key +
                            " value=" + value);
                    assertEquals(Integer.toString(mutationTable2), key.getField("a"));
                    assertEquals(1, key.getField("b"));
                    assertEquals(1, value.getField("c"));
                    mutationTable2++;
                }
            }
            assertEquals(4, mutationTable2);
        }

        result = pulsarContainer.execInContainer(
                "/pulsar/bin/pulsar-admin", "source", "delete",
                "--tenant", "public",
                "--namespace", "default",
                "--name", "cassandra-source-" + ksName + "-1");
        assertEquals(0, result.getExitCode());
        result = pulsarContainer.execInContainer(
                "/pulsar/bin/pulsar-admin", "source", "delete",
                "--tenant", "public",
                "--namespace", "default",
                "--name", "cassandra-source-" + ksName + "-2");
        assertEquals(0, result.getExitCode());
    }
}
