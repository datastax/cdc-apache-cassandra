package com.datastax.cdc.producer;

import com.datastax.cassandra.cdc.MutationValue;
import com.datastax.cassandra.cdc.SchemaRegistryContainer;
import com.datastax.cassandra.cdc.producer.KafkaMutationSender;
import com.datastax.cassandra.cdc.producer.PropertyConfig;
import com.datastax.oss.driver.api.core.CqlSession;
import com.google.common.collect.ImmutableMap;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
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
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.storage.StringConverter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collections;
import java.util.Locale;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class KafkaProducerTests {

    public static final String CASSANDRA_IMAGE = "cassandra:4.0-beta4";
    public static final String CONFLUENT_PLATFORM_VERSION = "5.5.1";

    static final String KAFKA_IMAGE = "confluentinc/cp-kafka:" + CONFLUENT_PLATFORM_VERSION;
    static final String KAFKA_SCHEMA_REGISTRY_IMAGE = "confluentinc/cp-schema-registry:" + CONFLUENT_PLATFORM_VERSION;
    static final String KAFKA_CONNECT_IMAGE = "confluentinc/cp-kafka-connect-base:" + CONFLUENT_PLATFORM_VERSION;

    private static Network testNetwork;
    private static CassandraContainer cassandraContainer;
    private static KafkaContainer kafkaContainer;
    private static SchemaRegistryContainer schemaRegistryContainer;

    static final int GIVE_UP = 100;

    @BeforeAll
    public static final void initBeforeClass() throws Exception {
        testNetwork = Network.newNetwork();

        kafkaContainer = new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE))
                .withNetwork(testNetwork)
                .withEmbeddedZookeeper()
                .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd.withName("kafka"))
                .withEnv("KAFKA_NUM_PARTITIONS","1");

        kafkaContainer.start();
        kafkaContainer.withStartupTimeout(Duration.ofSeconds(15));

        String internalBootstrapServers = String.format("PLAINTEXT://%s:%s", kafkaContainer.getContainerName(), 9092);
        schemaRegistryContainer = SchemaRegistryContainer
                .create(KAFKA_SCHEMA_REGISTRY_IMAGE, internalBootstrapServers)
                .withNetwork(testNetwork);
        schemaRegistryContainer.start();
        schemaRegistryContainer.withStartupTimeout(Duration.ofSeconds(15));

        String buildDir = System.getProperty("buildDir");
        cassandraContainer = new CassandraContainer(CASSANDRA_IMAGE)
                .withCreateContainerCmdModifier(c -> c.withName("cassandra"))
                .withLogConsumer(new Slf4jLogConsumer(log))
                .withNetwork(testNetwork)
                .withConfigurationOverride("cassandra-cdc")
                .withFileSystemBind(buildDir + "/libs/producer-v4-kafka-0.1-SNAPSHOT-all.jar","/producer-v4-kafka-0.1-SNAPSHOT-all.jar")
                .withEnv("JVM_EXTRA_OPTS", String.format(
                        Locale.ROOT,
                        "-javaagent:/producer-v4-kafka-0.1-SNAPSHOT-all.jar -DkafkaBrokers=%s -DschemaRegistryUrl=%s",
                        internalBootstrapServers,
                        schemaRegistryContainer.getRegistryUrlInDockerNetwork()));
        cassandraContainer.start();
        cassandraContainer.withStartupTimeout(Duration.ofSeconds(70));
    }

    @AfterAll
    public static void closeAfterClass() {
       cassandraContainer.close();
       kafkaContainer.close();
       schemaRegistryContainer.close();
    }

    KafkaConsumer<byte[], MutationValue> createConsumer(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryContainer.getRegistryUrl());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }

    @Test
    public void testAvroSimplePk() throws InterruptedException {
        try(CqlSession cqlSession = cassandraContainer.getCqlSession()) {
            cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS ks1 WITH replication = \n" +
                    "{'class':'SimpleStrategy','replication_factor':'1'};");
            cqlSession.execute("CREATE TABLE IF NOT EXISTS ks1.table1 (id text PRIMARY KEY, a int) WITH cdc=true");
            cqlSession.execute("INSERT INTO ks1.table1 (id, a) VALUES('1',1)");
            cqlSession.execute("INSERT INTO ks1.table1 (id, a) VALUES('2',1)");
            cqlSession.execute("INSERT INTO ks1.table1 (id, a) VALUES('3',1)");
        }
        Thread.sleep(15000);
        String topicName = PropertyConfig.topicPrefix + "ks1.table1";
        KafkaConsumer<byte[], MutationValue> consumer = createConsumer("test-consumer-avro-string-pk-group");
        consumer.subscribe(Collections.singleton(topicName));
        int noRecordsCount = 0;
        int mutationIndex= 1;

        // default AVRO key converter
        AvroConverter keyConverter = new AvroConverter();
        keyConverter.configure(
                ImmutableMap.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryContainer.getRegistryUrl()),
                true);
        Schema expectedKeySchema = SchemaBuilder.string().optional().build();

        while(true) {
            final ConsumerRecords<byte[], MutationValue> consumerRecords = consumer.poll(Duration.ofMillis(50));

            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > GIVE_UP) break;
            }

            for (ConsumerRecord<byte[], MutationValue> record : consumerRecords) {
                SchemaAndValue schemaAndValue = keyConverter.toConnectData(topicName, record.key());
                System.out.println("Consumer Record: partition=" + record.partition() +
                        " offset=" +record.offset() +
                        " key=" + schemaAndValue.value() +
                        " value="+ record.value());
                assertEquals(Integer.toString(mutationIndex), schemaAndValue.value());
                assertEquals(expectedKeySchema, schemaAndValue.schema());
                mutationIndex++;
            }
            consumer.commitSync();
        }
        System.out.println("noRecordsCount=" + noRecordsCount);
        assertEquals(4, mutationIndex);
        consumer.close();
    }


    @Test
    public void testStringSimplePk() throws InterruptedException {
        try(CqlSession cqlSession = cassandraContainer.getCqlSession()) {
            cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS ks1 WITH replication = \n" +
                    "{'class':'SimpleStrategy','replication_factor':'1'};");
            cqlSession.execute("CREATE TABLE IF NOT EXISTS ks1.table2 (id text PRIMARY KEY, a int) WITH cdc=true");
            cqlSession.execute(String.format(Locale.ROOT,
                    "UPDATE system_schema.tables SET extensions = { 'cdc.keyConverter': textAsBlob('%s')} WHERE keyspace_name = 'ks1' AND table_name='table2';",
                    StringConverter.class.getName()));

            cqlSession.execute("INSERT INTO ks1.table2 (id, a) VALUES('1',1)");
            cqlSession.execute("INSERT INTO ks1.table2 (id, a) VALUES('2',1)");
            cqlSession.execute("INSERT INTO ks1.table2 (id, a) VALUES('3',1)");
        }
        Thread.sleep(15000);
        String topicName = PropertyConfig.topicPrefix + "ks1.table2";
        KafkaConsumer<byte[], MutationValue> consumer = createConsumer("test-consumer-json-string-pk-group");
        consumer.subscribe(Collections.singleton(topicName));
        int noRecordsCount = 0;
        int mutationIndex= 1;

        StringConverter keyConverter = new StringConverter();
        while(true) {
            final ConsumerRecords<byte[], MutationValue> consumerRecords = consumer.poll(Duration.ofMillis(50));

            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > GIVE_UP) break;
            }

            for (ConsumerRecord<byte[], MutationValue> record : consumerRecords) {
                SchemaAndValue schemaAndValue = keyConverter.toConnectData(topicName, record.key());
                System.out.println("Consumer Record: partition=" + record.partition() +
                        " offset=" +record.offset() +
                        " key=" + schemaAndValue.value() +
                        " value="+ record.value());
                assertEquals(Integer.toString(mutationIndex), schemaAndValue.value());
                mutationIndex++;
            }
            consumer.commitSync();
        }
        System.out.println("noRecordsCount=" + noRecordsCount);
        assertEquals(4, mutationIndex);
        consumer.close();
    }

    @Test
    public void testAvroCompoundPk() throws InterruptedException {
        try(CqlSession cqlSession = cassandraContainer.getCqlSession()) {
            cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS ks1 WITH replication = \n" +
                    "{'class':'SimpleStrategy','replication_factor':'1'};");
            cqlSession.execute("CREATE TABLE IF NOT EXISTS ks1.table3 (a text, b int, c int, PRIMARY KEY(a,b)) WITH cdc=true");
            cqlSession.execute("INSERT INTO ks1.table3 (a,b,c) VALUES('1',1,1)");
            cqlSession.execute("INSERT INTO ks1.table3 (a,b,c) VALUES('2',1,1)");
            cqlSession.execute("INSERT INTO ks1.table3 (a,b,c) VALUES('3',1,1)");
        }
        Thread.sleep(15000);
        String topicName = PropertyConfig.topicPrefix + "ks1.table3";
        KafkaConsumer<byte[], MutationValue> consumer = createConsumer("test-consumer-avro-compound-pk-group");
        consumer.subscribe(Collections.singleton(topicName));
        int noRecordsCount = 0;
        int mutationIndex= 1;

        AvroConverter keyConverter = new AvroConverter();
        keyConverter.configure(
                ImmutableMap.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryContainer.getRegistryUrl()),
                true);
        Schema expectedKeySchema = SchemaBuilder.struct()
                .name("ks1.table3")
                .version(1)
                .doc(KafkaMutationSender.SCHEMA_DOC_PREFIX + "ks1.table3")
                .field("a", SchemaBuilder.string().optional().build())
                .field("b", SchemaBuilder.int32().optional().build())
                .build();

        while(true) {
            final ConsumerRecords<byte[], MutationValue> consumerRecords = consumer.poll(Duration.ofMillis(50));

            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > GIVE_UP) break;
            }

            for (ConsumerRecord<byte[], MutationValue> record : consumerRecords) {
                SchemaAndValue keySchemaAndValue = keyConverter.toConnectData(topicName, record.key());
                System.out.println("Consumer Record: partition=" + record.partition() +
                        " offset=" +record.offset() +
                        " key=" + keySchemaAndValue.value() +
                        " value="+ record.value());
                assertEquals(expectedKeySchema, keySchemaAndValue.schema());
                Struct expectedKey = new Struct(keySchemaAndValue.schema())
                        .put("a", Integer.toString(mutationIndex))
                        .put("b", 1);
                assertEquals(expectedKey, keySchemaAndValue.value());
                mutationIndex++;
            }
            consumer.commitSync();
        }
        System.out.println("noRecordsCount=" + noRecordsCount);
        assertEquals(4, mutationIndex);
        consumer.close();
    }

    @Test
    public void testJsonCompoundPk() throws InterruptedException {
        try(CqlSession cqlSession = cassandraContainer.getCqlSession()) {
            cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS ks1 WITH replication = \n" +
                    "{'class':'SimpleStrategy','replication_factor':'1'};");
            cqlSession.execute("CREATE TABLE IF NOT EXISTS ks1.table4 (a text, b int, c int, PRIMARY KEY(a,b)) WITH cdc=true");
            cqlSession.execute(String.format(Locale.ROOT,
                    "UPDATE system_schema.tables SET extensions = { 'cdc.keyConverter': textAsBlob('%s')} WHERE keyspace_name = 'ks1' AND table_name='table4';",
                    JsonConverter.class.getName()));
            cqlSession.execute("INSERT INTO ks1.table4 (a,b,c) VALUES('1',1,1)");
            cqlSession.execute("INSERT INTO ks1.table4 (a,b,c) VALUES('2',1,1)");
            cqlSession.execute("INSERT INTO ks1.table4 (a,b,c) VALUES('3',1,1)");
        }
        Thread.sleep(15000);
        String topicName = PropertyConfig.topicPrefix + "ks1.table3";
        KafkaConsumer<byte[], MutationValue> consumer = createConsumer("test-consumer-avro-compound-pk-group");
        consumer.subscribe(Collections.singleton(topicName));
        int noRecordsCount = 0;
        int mutationIndex= 1;

        JsonConverter keyConverter = new JsonConverter();
        keyConverter.configure(
                ImmutableMap.of(
                        JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false,
                        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryContainer.getRegistryUrl()),
                true);
        Schema expectedKeySchema = SchemaBuilder.struct()
                .name(topicName)
                .doc("Primary key schema for table "+topicName)
                .field("a", SchemaBuilder.string().optional().build())
                .field("b", SchemaBuilder.int64().optional().build())
                .build();

        while(true) {
            final ConsumerRecords<byte[], MutationValue> consumerRecords = consumer.poll(Duration.ofMillis(50));

            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > GIVE_UP) break;
            }

            for (ConsumerRecord<byte[], MutationValue> record : consumerRecords) {
                SchemaAndValue keySchemaAndValue = keyConverter.toConnectData(topicName, record.key());
                System.out.println("Consumer Record: partition=" + record.partition() +
                        " offset=" +record.offset() +
                        " key=" + keySchemaAndValue.value() +
                        " value="+ record.value());
                assertEquals(expectedKeySchema, keySchemaAndValue.schema());
                Struct expectedKey = new Struct(keySchemaAndValue.schema())
                        .put("a", Integer.toString(mutationIndex))
                        .put("b", 1);
                assertEquals(expectedKey, keySchemaAndValue.value());
                mutationIndex++;
            }
            consumer.commitSync();
        }
        System.out.println("noRecordsCount=" + noRecordsCount);
        assertEquals(4, mutationIndex);
        consumer.close();
    }
}
