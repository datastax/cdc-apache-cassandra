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
import org.apache.pulsar.client.impl.schema.AvroSchema;
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
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class PulsarProducerTests {

    public static final String CASSANDRA_IMAGE = "cassandra:4.0-beta4";
    public static final String PULSAR_VERSION = "latest";

    static final String PULSAR_IMAGE = "harbor.sjc.dsinternal.org/pulsar/pulsar-all:" + PULSAR_VERSION;

    private static Network testNetwork;
    private static CassandraContainer<?> cassandraContainer;
    private static PulsarContainer<?> pulsarContainer;

    @BeforeAll
    public static final void initBeforeClass() throws Exception {
        testNetwork = Network.newNetwork();

        pulsarContainer = new PulsarContainer<>(DockerImageName.parse(PULSAR_IMAGE))
                .withNetwork(testNetwork)
                .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd.withName("pulsar"))
                .withStartupTimeout(Duration.ofSeconds(30));
        pulsarContainer.start();

        String buildDir = System.getProperty("buildDir");
        String projectVersion = System.getProperty("projectVersion");
        String jarFile = String.format(Locale.ROOT, "producer-v4-pulsar-%s-all.jar", projectVersion);
        cassandraContainer = new CassandraContainer<>(CASSANDRA_IMAGE)
                .withCreateContainerCmdModifier(c -> c.withName("cassandra"))
                .withLogConsumer(new Slf4jLogConsumer(log))
                .withNetwork(testNetwork)
                .withConfigurationOverride("cassandra-cdc")
                .withFileSystemBind(
                        String.format(Locale.ROOT, "%s/libs/%s", buildDir, jarFile),
                        String.format(Locale.ROOT, "/%s", jarFile))
                .withEnv("JVM_EXTRA_OPTS", String.format(
                        Locale.ROOT,
                        "-javaagent:/%s -DpulsarServiceUrl=%s",
                        jarFile,
                        "http://pulsar:" + pulsarContainer.BROKER_PORT))
                .withStartupTimeout(Duration.ofSeconds(70));
        cassandraContainer.start();
    }

    @AfterAll
    public static void closeAfterAll() {
       cassandraContainer.close();
       pulsarContainer.close();
    }

    @Test
    public void testProducer() throws InterruptedException, PulsarClientException {
        try(CqlSession cqlSession = cassandraContainer.getCqlSession()) {
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
        int mutationTable1= 1;
        int mutationTable2= 1;

        try(PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(ProducerConfig.pulsarServiceUrl).build()) {
            Schema<KeyValue<String, MutationValue>> schema1 = KeyValueSchema.of(
                    Schema.STRING,
                    AvroSchema.of(MutationValue.class),
                    KeyValueEncodingType.SEPARATED);

            try (Consumer<KeyValue<String, MutationValue>> consumer = pulsarClient.newConsumer(schema1)
                    .topic("events-ks1.table1")
                    .subscriptionName("sub1")
                    .subscriptionType(SubscriptionType.Key_Shared)
                    .subscriptionMode(SubscriptionMode.Durable)
                    .subscribe()) {
                Message<KeyValue<String, MutationValue>> msg = consumer.receive();
                KeyValue<String, MutationValue> kv = msg.getValue();
                String key = kv.getKey();
                MutationValue val = kv.getValue();
                System.out.println("Consumer Record: topicName=" + msg.getTopicName() +
                        " key=" + key +
                        " value=" + val);
                assertEquals(Integer.toString(mutationTable1), key);
                mutationTable1++;
            }
            assertEquals(4, mutationTable1);

            RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("ks1.table1");
            recordSchemaBuilder.field("a").type(SchemaType.STRING).optional().defaultValue(null);
            recordSchemaBuilder.field("b").type(SchemaType.INT32).optional().defaultValue(null);
            SchemaInfo keySchemaInfo = recordSchemaBuilder.build(SchemaType.AVRO);
            Schema<GenericRecord> keySchema = GenericSchemaImpl.of(keySchemaInfo);
            Schema<KeyValue<GenericRecord, MutationValue>> schema2 = KeyValueSchema.of(
                    keySchema,
                    AvroSchema.of(MutationValue.class),
                    KeyValueEncodingType.SEPARATED);
            try (Consumer<KeyValue<GenericRecord, MutationValue>> consumer = pulsarClient.newConsumer(schema2)
                    .topic("events-ks1.table2")
                    .subscriptionName("sub1")
                    .subscriptionType(SubscriptionType.Key_Shared)
                    .subscriptionMode(SubscriptionMode.Durable)
                    .subscribe()) {
                Message<KeyValue<GenericRecord, MutationValue>> msg = consumer.receive();
                KeyValue<GenericRecord, MutationValue> kv = msg.getValue();
                GenericRecord key = kv.getKey();
                MutationValue val = kv.getValue();
                System.out.println("Consumer Record: topicName=" + msg.getTopicName() +
                        " key=" + key +
                        " value=" + val);
                GenericAvroSchema genericAvroSchema = new GenericAvroSchema(keySchemaInfo);
                GenericRecord expectedKey = genericAvroSchema.newRecordBuilder()
                        .set("a", Integer.toString(mutationTable2))
                        .set("b", 1)
                        .build();
                assertEquals(expectedKey, key);
                mutationTable2++;
            }
            assertEquals(4, mutationTable2);
        }
    }
}
