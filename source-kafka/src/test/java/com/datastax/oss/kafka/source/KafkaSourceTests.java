package com.datastax.oss.kafka.source;

import com.datastax.cassandra.cdc.producer.KafkaMutationSender;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.testcontainers.cassandra.CassandraContainer;
import com.datastax.testcontainers.kafka.KafkaConnectContainer;
import com.datastax.testcontainers.kafka.SchemaRegistryContainer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.Converter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.shaded.org.apache.commons.lang.RandomStringUtils;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.time.Duration;
import java.util.Locale;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class KafkaSourceTests {

    public static final String CASSANDRA_IMAGE = "cassandra:4.0-beta4";
    public static final String CONFLUENT_PLATFORM_VERSION = "5.5.1";

    static final String KAFKA_IMAGE = "confluentinc/cp-kafka:" + CONFLUENT_PLATFORM_VERSION;
    static final String KAFKA_SCHEMA_REGISTRY_IMAGE = "confluentinc/cp-schema-registry:" + CONFLUENT_PLATFORM_VERSION;
    static final String KAFKA_CONNECT_IMAGE = "confluentinc/cp-kafka-connect-base:" + CONFLUENT_PLATFORM_VERSION;

    private static String seed = "1";   // must match the source connector config
    private static Network testNetwork;
    private static CassandraContainer<?> cassandraContainer;
    private static KafkaContainer kafkaContainer;
    private static SchemaRegistryContainer schemaRegistryContainer;
    private static KafkaConnectContainer kafkaConnectContainer;

    static final int GIVE_UP = 100;

    @BeforeAll
    public static final void initBeforeClass() throws Exception {
        testNetwork = Network.newNetwork();

        kafkaContainer = new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE))
                .withNetwork(testNetwork)
                .withEmbeddedZookeeper()
                .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd.withName("kafka-" + seed))
                .withEnv("KAFKA_NUM_PARTITIONS", "1")
                .withStartupTimeout(Duration.ofSeconds(30));
        kafkaContainer.start();

        String internalBootstrapServers = String.format("PLAINTEXT:/%s:%s", kafkaContainer.getContainerName(), 9092);
        schemaRegistryContainer = SchemaRegistryContainer
                .create(KAFKA_SCHEMA_REGISTRY_IMAGE, seed, internalBootstrapServers)
                .withNetwork(testNetwork)
                .withStartupTimeout(Duration.ofSeconds(30));
        schemaRegistryContainer.start();

        String producerBuildDir = System.getProperty("producerBuildDir");
        String sourceBuildDir = System.getProperty("sourceBuildDir");
        String projectVersion = System.getProperty("projectVersion");
        String producerJarFile = String.format(Locale.ROOT, "producer-v4-kafka-%s-all.jar", projectVersion);
        String sourceJarFile = String.format(Locale.ROOT, "source-kafka-%s-all.jar", projectVersion);
        cassandraContainer = new CassandraContainer<>(CASSANDRA_IMAGE)
                .withCreateContainerCmdModifier(c -> c.withName("cassandra-" + seed))
                .withLogConsumer(new Slf4jLogConsumer(log))
                .withNetwork(testNetwork)
                .withConfigurationOverride("cassandra-cdc")
                .withFileSystemBind(
                        String.format(Locale.ROOT, "%s/libs/%s", producerBuildDir, producerJarFile),
                        String.format(Locale.ROOT, "/%s", producerJarFile))
                .withEnv("JVM_EXTRA_OPTS", String.format(
                        Locale.ROOT,
                        "-javaagent:/%s=kafkaBrokers=%s,kafkaSchemaRegistryUrl=%s",
                        producerJarFile,
                        internalBootstrapServers,
                        schemaRegistryContainer.getRegistryUrlInDockerNetwork()))
                .withStartupTimeout(Duration.ofSeconds(120));
        cassandraContainer.start();

        kafkaConnectContainer = KafkaConnectContainer
                .create(KAFKA_CONNECT_IMAGE, seed, internalBootstrapServers, schemaRegistryContainer.getRegistryUrlInDockerNetwork())
                .withNetwork(testNetwork)
                .withFileSystemBind(
                        String.format(Locale.ROOT, "%s/libs/%s", sourceBuildDir, sourceJarFile),
                        String.format(Locale.ROOT, "/connect-plugins/%s", sourceJarFile))
                .withStartupTimeout(Duration.ofSeconds(180));
        kafkaConnectContainer.start();
    }

    @AfterAll
    public static void closeAfterAll() {
        cassandraContainer.close();
        kafkaConnectContainer.close();
        schemaRegistryContainer.close();
        kafkaContainer.close();
    }

    KafkaConsumer<byte[], byte[]> createConsumer(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryContainer.getRegistryUrl());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArrayDeserializer.class.getName());
        props.put("max.poll.interval.ms","60000");  // because we need to wait synced commitlogs on disk
        return new KafkaConsumer<>(props);
    }

    @Test
    public void testWithAvroConverters() throws InterruptedException, IOException {
        Converter keyConverter = new AvroConverter();
        keyConverter.configure(
                ImmutableMap.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryContainer.getRegistryUrl()),
                true);
        Converter valueConverter = new AvroConverter();
        valueConverter.configure(
                ImmutableMap.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryContainer.getRegistryUrl()),
                false);
        testSourceConnector("ks1", keyConverter, valueConverter, false);
    }

    @Test
    public void testWithJsonConverters() throws InterruptedException, IOException {
        Converter keyConverter = new JsonConverter();
        keyConverter.configure(
                ImmutableMap.of(
                        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryContainer.getRegistryUrl(),
                        "key.converter.schemas.enable", "true"),
                true);
        Converter valueConverter = new JsonConverter();
        valueConverter.configure(
                ImmutableMap.of(
                        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryContainer.getRegistryUrl(),
                        "value.converter.schemas.enable", "true"),
                false);
        testSourceConnector("ks2", keyConverter, valueConverter, true);
    }

    public void testSourceConnector(String ksName, Converter keyConverter, Converter valueConverter, boolean schemaWithNullValue) throws InterruptedException, IOException {
        try(CqlSession cqlSession = cassandraContainer.getCqlSession()) {
            cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS " + ksName +
                    " WITH replication = {'class':'SimpleStrategy','replication_factor':'1'};");
            cqlSession.execute("CREATE TABLE IF NOT EXISTS "+ksName+".table1 (id text PRIMARY KEY, a int) WITH cdc=true");
            cqlSession.execute("INSERT INTO "+ksName+".table1 (id, a) VALUES('1',1)");
            cqlSession.execute("INSERT INTO "+ksName+".table1 (id, a) VALUES('2',1)");
            cqlSession.execute("INSERT INTO "+ksName+".table1 (id, a) VALUES('3',1)");

            cqlSession.execute("CREATE TABLE IF NOT EXISTS "+ksName+".table2 (a text, b int, c int, PRIMARY KEY(a,b)) WITH cdc=true");
            cqlSession.execute("INSERT INTO "+ksName+".table2 (a,b,c) VALUES('1',1,1)");
            cqlSession.execute("INSERT INTO "+ksName+".table2 (a,b,c) VALUES('2',1,1)");
            cqlSession.execute("INSERT INTO "+ksName+".table2 (a,b,c) VALUES('3',1,1)");
        }

        kafkaConnectContainer.setLogging("com.datastax","DEBUG");

        // source connector must be deployed after the creation of the cassandra table.
        assertEquals(201, kafkaConnectContainer.deployConnector("source-cassandra-"+ksName+"-table1.yaml"));
        assertEquals(201, kafkaConnectContainer.deployConnector("source-cassandra-"+ksName+"-table2.yaml"));

        // wait commitlogs sync on disk
        Thread.sleep(11000);
        KafkaConsumer<byte[], byte[]> consumer = createConsumer("test-consumer-data-group-"+ksName);
        consumer.subscribe(ImmutableList.of("data-"+ksName+".table1", "data-"+ksName+".table2"));
        int noRecordsCount = 0;
        int mutationTable1 = 1;
        int mutationTable2 = 1;


        Schema expectedKeySchema1 = SchemaBuilder.string().optional().build();
        Schema expectedKeySchema2 = SchemaBuilder.struct()
                .name(ksName+".table2")
                .version(1)
                .doc(KafkaMutationSender.SCHEMA_DOC_PREFIX + ksName+".table2")
                .field("a", SchemaBuilder.string().optional().build())
                .field("b", SchemaBuilder.int32().optional().build())
                .build();

        Schema expectedValueSchema1 = SchemaBuilder.struct()
                .name(ksName + ".table1")
                .doc(CassandraConverter.TABLE_SCHEMA_DOC_PREFIX + ksName+".table1")
                .optional()
                .field("id", SchemaBuilder.string().optional().build())
                .field("a", SchemaBuilder.int32().optional().build())
                .build();
        Schema expectedValueSchema2 = SchemaBuilder.struct()
                .name(ksName + ".table2")
                .doc(CassandraConverter.TABLE_SCHEMA_DOC_PREFIX + ksName+".table2")
                .optional()
                .field("a", SchemaBuilder.string().optional().build())
                .field("b", SchemaBuilder.int32().optional().build())
                .field("c", SchemaBuilder.int32().optional().build())
                .build();

        while(true) {
            final ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(Duration.ofMillis(100));
            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > GIVE_UP) break;
            }
            for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
                String topicName = record.topic();
                SchemaAndValue keySchemaAndValue = keyConverter.toConnectData(topicName, record.key());
                SchemaAndValue valueSchemaAndValue = valueConverter.toConnectData(topicName, record.value());
                System.out.println("Consumer Record: topicName=" + topicName+
                        " partition=" + record.partition() +
                        " offset=" +record.offset() +
                        " key=" + keySchemaAndValue.value() +
                        " value="+ valueSchemaAndValue.value());
                System.out.println("key schema: " + CassandraConverter.schemaToString(keySchemaAndValue.schema()));
                System.out.println("value schema: " + CassandraConverter.schemaToString(valueSchemaAndValue.schema()));
                if (topicName.endsWith("table1")) {
                    assertEquals(Integer.toString(mutationTable1), keySchemaAndValue.value());
                    assertEquals(expectedKeySchema1, keySchemaAndValue.schema());
                    Struct expectedValue = new Struct(valueSchemaAndValue.schema())
                            .put("id", Integer.toString(mutationTable1))
                            .put("a", 1);
                    assertEquals(expectedValue, valueSchemaAndValue.value());
                    assertEquals(expectedValueSchema1, valueSchemaAndValue.schema());
                    mutationTable1++;
                } else if (topicName.endsWith("table2")) {
                    Struct expectedKey = new Struct(keySchemaAndValue.schema())
                            .put("a", Integer.toString(mutationTable2))
                            .put("b", 1);
                    Struct expectedValue = new Struct(valueSchemaAndValue.schema())
                            .put("a", Integer.toString(mutationTable2))
                            .put("b", 1)
                            .put("c", 1);
                    assertEquals(expectedKey, keySchemaAndValue.value());
                    assertEquals(expectedKeySchema2, keySchemaAndValue.schema());
                    assertEquals(expectedValue, valueSchemaAndValue.value());
                    assertEquals(expectedValueSchema2, valueSchemaAndValue.schema());
                    mutationTable2++;
                }
            }
            consumer.commitSync();
        }
        assertEquals(4, mutationTable1);
        assertEquals(4, mutationTable2);

        // trigger a schema update
        try(CqlSession cqlSession = cassandraContainer.getCqlSession()) {
            cqlSession.execute("ALTER TABLE "+ksName+".table1 ADD b double");
            cqlSession.execute("INSERT INTO "+ksName+".table1 (id,a,b) VALUES('1',1,1.0)");

            cqlSession.execute("CREATE TYPE "+ksName+".type2 (a bigint, b smallint);");
            cqlSession.execute("ALTER TABLE "+ksName+".table2 ADD d type2");
            cqlSession.execute("INSERT INTO "+ksName+".table2 (a,b,c,d) VALUES('1',1,1,{a:1,b:1})");
        }
        // wait commitlogs sync on disk
        Thread.sleep(11000);

        Schema expectedValueSchema1v2 = SchemaBuilder.struct()
                .name(ksName+".table1")
                .doc(CassandraConverter.TABLE_SCHEMA_DOC_PREFIX + ksName+".table1")
                .optional()
                .field("id", SchemaBuilder.string().optional().build())
                .field("a", SchemaBuilder.int32().optional().build())
                .field("b", SchemaBuilder.float64().optional().build())
                .build();
        Schema type2Schema = SchemaBuilder.struct()
                .name(ksName+".type2")
                .doc(CassandraConverter.TYPE_SCHEMA_DOC_PREFIX + ksName+".type2")
                .optional()
                .field("a", SchemaBuilder.int64().optional().build())
                .field("b", SchemaBuilder.int16().optional().build())
                .build();
        Schema expectedValueSchema2v2 = SchemaBuilder.struct()
                .name(ksName+".table2")
                .doc(CassandraConverter.TABLE_SCHEMA_DOC_PREFIX + ksName+".table2")
                .optional()
                .field("a", SchemaBuilder.string().optional().build())
                .field("b", SchemaBuilder.int32().optional().build())
                .field("c", SchemaBuilder.int32().optional().build())
                .field("d", type2Schema)
                .build();
        noRecordsCount = 0;
        while(true) {
            final ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(Duration.ofMillis(100));
            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > GIVE_UP) break;
            }
            for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
                String topicName = record.topic();
                SchemaAndValue keySchemaAndValue = keyConverter.toConnectData(topicName, record.key());
                SchemaAndValue valueSchemaAndValue = valueConverter.toConnectData(topicName, record.value());
                System.out.println("Consumer Record: topicName=" + topicName+
                        " partition=" + record.partition() +
                        " offset=" +record.offset() +
                        " key=" + keySchemaAndValue.value() +
                        " value="+ valueSchemaAndValue.value());
                System.out.println("key schema: " + CassandraConverter.schemaToString(keySchemaAndValue.schema()));
                System.out.println("value schema: " + CassandraConverter.schemaToString(valueSchemaAndValue.schema()));
                if (topicName.endsWith("table1")) {
                    assertEquals("1", keySchemaAndValue.value());
                    assertEquals(expectedKeySchema1, keySchemaAndValue.schema());
                    Struct expectedValue = new Struct(valueSchemaAndValue.schema())
                            .put("id", "1")
                            .put("a", 1)
                            .put("b", 1.0d);
                    assertEquals(expectedValue, valueSchemaAndValue.value());
                    assertEquals(expectedValueSchema1v2, valueSchemaAndValue.schema());
                    mutationTable1++;
                } else if (topicName.endsWith("table2")) {
                    Struct expectedKey = new Struct(keySchemaAndValue.schema())
                            .put("a", "1")
                            .put("b", 1);
                    Struct expectedValue = new Struct(valueSchemaAndValue.schema())
                            .put("a", "1")
                            .put("b", 1)
                            .put("c", 1)
                            .put("d", new Struct(type2Schema)
                                    .put("a", 1L)
                                    .put("b", (short)1));
                    assertEquals(expectedKey, keySchemaAndValue.value());
                    assertEquals(expectedKeySchema2, keySchemaAndValue.schema());
                    assertEquals(expectedValue, valueSchemaAndValue.value());
                    assertEquals(expectedValueSchema2v2, valueSchemaAndValue.schema());
                    mutationTable2++;
                }
            }
            consumer.commitSync();
        }
        assertEquals(5, mutationTable1);
        assertEquals(5, mutationTable2);

        // delete rows
        try(CqlSession cqlSession = cassandraContainer.getCqlSession()) {
            cqlSession.execute("DELETE FROM "+ksName+".table1 WHERE id = '1'");
            cqlSession.execute("DELETE FROM "+ksName+".table2 WHERE a = '1' AND b = 1");
        }
        // wait commitlogs sync on disk
        Thread.sleep(11000);

        noRecordsCount = 0;
        while(true) {
            final ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(Duration.ofMillis(100));
            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > GIVE_UP) break;
            }
            for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
                String topicName = record.topic();
                SchemaAndValue keySchemaAndValue = keyConverter.toConnectData(topicName, record.key());
                SchemaAndValue valueSchemaAndValue = valueConverter.toConnectData(topicName, record.value());

                System.out.println("Consumer Record: topicName=" + topicName+
                        " partition=" + record.partition() +
                        " offset=" +record.offset() +
                        " key=" + keySchemaAndValue.value() +
                        " value="+ valueSchemaAndValue.value());
                System.out.println("key schema: " + CassandraConverter.schemaToString(keySchemaAndValue.schema()));
                if (valueSchemaAndValue.schema() != null)
                    System.out.println("value schema: " + CassandraConverter.schemaToString(valueSchemaAndValue.schema()));

                if (topicName.endsWith("table1")) {
                    assertEquals("1", keySchemaAndValue.value());
                    assertEquals(expectedKeySchema1, keySchemaAndValue.schema());
                    assertEquals(null, valueSchemaAndValue.value());
                    if (schemaWithNullValue) {
                        assertEquals(expectedValueSchema1v2, valueSchemaAndValue.schema());
                    } else {
                        assertEquals(null, valueSchemaAndValue.schema());
                    }
                    mutationTable1++;
                } else if (topicName.endsWith("table2")) {
                    Struct expectedKey = new Struct(keySchemaAndValue.schema())
                            .put("a", "1")
                            .put("b", 1);
                    assertEquals(expectedKey, keySchemaAndValue.value());
                    assertEquals(expectedKeySchema2, keySchemaAndValue.schema());
                    assertEquals(null, valueSchemaAndValue.value());
                    if (schemaWithNullValue) {
                        assertEquals(expectedValueSchema2v2, valueSchemaAndValue.schema());
                    } else {
                        assertEquals(null, valueSchemaAndValue.schema());
                    }
                    mutationTable2++;
                }
            }
            consumer.commitSync();
        }
        assertEquals(6, mutationTable1);
        assertEquals(6, mutationTable2);

        consumer.close();
        assertEquals(204, kafkaConnectContainer.undeployConnector("cassandra-source-"+ksName+"-table1"));
        assertEquals(204, kafkaConnectContainer.undeployConnector("cassandra-source-"+ksName+"-table2"));
    }
}
