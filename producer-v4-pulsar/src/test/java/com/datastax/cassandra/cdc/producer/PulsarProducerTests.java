package com.datastax.cassandra.cdc.producer;

import com.datastax.cassandra.cdc.MutationValue;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.testcontainers.cassandra.CassandraContainer;
import com.datastax.testcontainers.pulsar.PulsarContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.client.impl.schema.KeyValueSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericSchemaImpl;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class PulsarProducerTests {

    public static final String CASSANDRA_IMAGE = "cassandra:4.0-beta4";
    public static final String PULSAR_VERSION = "latest";

    static final String PULSAR_IMAGE = "strapdata/pulsar-all:" + PULSAR_VERSION;

    private static Network testNetwork;
    private static PulsarContainer<?> pulsarContainer;

    @BeforeAll
    public static final void initBeforeClass() throws Exception {
        testNetwork = Network.newNetwork();

        pulsarContainer = new PulsarContainer<>(DockerImageName.parse(PULSAR_IMAGE))
                .withNetwork(testNetwork)
                .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd.withName("pulsar"))
                .withStartupTimeout(Duration.ofSeconds(30));
        pulsarContainer.start();
    }

    @AfterAll
    public static void closeAfterAll() {
        pulsarContainer.close();
    }

    @Test
    public void testProducer() throws InterruptedException, IOException {
        org.testcontainers.containers.Container.ExecResult result = pulsarContainer.execInContainer(
                "/pulsar/bin/pulsar-admin", "namespaces", "set-auto-topic-creation",
                "public/default", "--enable", "--type", "partitioned", "--num-partitions", "1");
        assertEquals(0, result.getExitCode());
        result = pulsarContainer.execInContainer(
                "/pulsar/bin/pulsar-admin", "namespaces", "set-is-allow-auto-update-schema", "public/default", "--enable");
        assertEquals(0, result.getExitCode());
        result = pulsarContainer.execInContainer(
                "/pulsar/bin/pulsar-admin", "namespaces", "set-deduplication", "public/default", "--enable");
        assertEquals(0, result.getExitCode());

        String buildDir = System.getProperty("buildDir");
        String projectVersion = System.getProperty("projectVersion");
        String jarFile = String.format(Locale.ROOT, "producer-v4-pulsar-%s-all.jar", projectVersion);
        try (CassandraContainer<?> cassandraContainer = new CassandraContainer<>(CASSANDRA_IMAGE)
                .withCreateContainerCmdModifier(c -> c.withName("cassandra"))
                .withNetwork(testNetwork)
                .withConfigurationOverride("cassandra-cdc")
                .withFileSystemBind(
                        String.format(Locale.ROOT, "%s/libs/%s", buildDir, jarFile),
                        String.format(Locale.ROOT, "/%s", jarFile))
                .withEnv("JVM_EXTRA_OPTS", String.format(
                        Locale.ROOT,
                        "-javaagent:/%s=pulsarServiceUrl=%s",
                        jarFile,
                        "pulsar://pulsar:" + pulsarContainer.BROKER_PORT))
                .withStartupTimeout(Duration.ofSeconds(70))) {
            cassandraContainer.start();

            try (CqlSession cqlSession = cassandraContainer.getCqlSession()) {
                cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS ks1 WITH replication = \n" +
                        "{'class':'SimpleStrategy','replication_factor':'1'};");
                cqlSession.execute("CREATE TABLE IF NOT EXISTS ks1.table1 (id text PRIMARY KEY, a int) WITH cdc=true");
                cqlSession.execute("INSERT INTO ks1.table1 (id, a) VALUES('1',1)");
                cqlSession.execute("INSERT INTO ks1.table1 (id, a) VALUES('2',1)");
                cqlSession.execute("INSERT INTO ks1.table1 (id, a) VALUES('3',1)");

                cqlSession.execute("CREATE TABLE IF NOT EXISTS ks1.table2 (a text, b int, c int, PRIMARY KEY(a,b)) WITH cdc=true");
                cqlSession.execute("INSERT INTO ks1.table2 (a,b,c) VALUES('1',1,1)");
                cqlSession.execute("INSERT INTO ks1.table2 (a,b,c) VALUES('2',1,1)");
                cqlSession.execute("INSERT INTO ks1.table2 (a,b,c) VALUES('3',1,1)");
            }

            Thread.sleep(15000);
            int mutationTable1 = 1;
            int mutationTable2 = 1;

            try (PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(pulsarContainer.getPulsarBrokerUrl()).build()) {
                Schema<KeyValue<String, MutationValue>> schema1 = KeyValueSchema.of(
                        Schema.STRING,
                        Schema.AVRO(MutationValue.class),
                        KeyValueEncodingType.SEPARATED);

                // pulsar-admin schemas get "persistent://public/default/events-ks1.table1"
                // pulsar-admin topics peek-messages persistent://public/default/events-ks1.table1-partition-0 --count 3 --subscription sub1
                try (Consumer<KeyValue<String, MutationValue>> consumer = pulsarClient.newConsumer(schema1)
                        .topic("events-ks1.table1-partition-0")
                        .subscriptionName("sub1")
                        .subscriptionType(SubscriptionType.Key_Shared)
                        .subscriptionMode(SubscriptionMode.Durable)
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                        .subscribe()) {
                    Message<KeyValue<String, MutationValue>> msg;
                    while ((msg = consumer.receive(30, TimeUnit.SECONDS)) != null && mutationTable1 < 4) {
                        KeyValue<String, MutationValue> kv = msg.getValue();
                        String key = kv.getKey();
                        MutationValue val = kv.getValue();
                        System.out.println("Consumer Record: topicName=" + msg.getTopicName() +
                                " key=" + key +
                                " value=" + val);
                        assertEquals(Integer.toString(mutationTable1), key);
                        mutationTable1++;
                    }
                }
                assertEquals(4, mutationTable1);

                // pulsar-admin schemas get "persistent://public/default/events-ks1.table2"
                // pulsar-admin topics peek-messages persistent://public/default/events-ks1.table2-partition-0 --count 3 --subscription sub1
                RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("ks1.table2");
                recordSchemaBuilder.field("a").type(SchemaType.STRING).optional().defaultValue(null);
                recordSchemaBuilder.field("b").type(SchemaType.INT32).optional().defaultValue(null);
                SchemaInfo keySchemaInfo = recordSchemaBuilder.build(SchemaType.AVRO);
                Schema<GenericRecord> keySchema = GenericSchemaImpl.of(keySchemaInfo);
                Schema<KeyValue<GenericRecord, MutationValue>> schema2 = KeyValueSchema.of(
                        keySchema,
                        Schema.AVRO(MutationValue.class),
                        KeyValueEncodingType.SEPARATED);
                try (Consumer<KeyValue<GenericRecord, MutationValue>> consumer = pulsarClient.newConsumer(schema2)
                        .topic("events-ks1.table2-partition-0")
                        .subscriptionName("sub1")
                        .subscriptionType(SubscriptionType.Key_Shared)
                        .subscriptionMode(SubscriptionMode.Durable)
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                        .subscribe()) {
                    Message<KeyValue<GenericRecord, MutationValue>> msg;
                    while ((msg = consumer.receive(30, TimeUnit.SECONDS)) != null && mutationTable2 < 4) {
                        KeyValue<GenericRecord, MutationValue> kv = msg.getValue();
                        GenericRecord key = kv.getKey();
                        MutationValue val = kv.getValue();
                        System.out.println("Consumer Record: topicName=" + msg.getTopicName() +
                                " key=" + key +
                                " value=" + val);
                        assertEquals(Integer.toString(mutationTable2), key.getField("a"));
                        assertEquals(1, key.getField("b"));
                        mutationTable2++;
                    }
                }
                assertEquals(4, mutationTable2);
            }
        }
    }
}
