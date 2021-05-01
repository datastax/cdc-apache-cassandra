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
package com.datastax.cdc.producer;

import com.datastax.cassandra.cdc.MutationValue;
import com.datastax.cassandra.cdc.producer.KafkaMutationSender;
import com.datastax.cassandra.cdc.producer.ProducerConfig;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.testcontainers.cassandra.CassandraContainer;
import com.datastax.testcontainers.kafka.SchemaRegistryContainer;
import com.google.common.collect.ImmutableList;
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
public class KafkaV3ProducerTests {

    public static final String CASSANDRA_IMAGE = "cassandra:3.11.10";
    public static final String CONFLUENT_PLATFORM_VERSION = "5.5.1";

    static final String KAFKA_IMAGE = "confluentinc/cp-kafka:" + CONFLUENT_PLATFORM_VERSION;
    static final String KAFKA_SCHEMA_REGISTRY_IMAGE = "confluentinc/cp-schema-registry:" + CONFLUENT_PLATFORM_VERSION;

    private static String seed;
    private static Network testNetwork;
    private static KafkaContainer kafkaContainer;
    private static SchemaRegistryContainer schemaRegistryContainer;
    private static String internalBootstrapServers;

    static final int GIVE_UP = 100;

    @BeforeAll
    public static final void initBeforeClass() throws Exception {
        testNetwork = Network.newNetwork();

        // seed to uniquely identify containers between concurrent tests.
        seed = RandomStringUtils.randomNumeric(6);

        kafkaContainer = new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE))
                .withNetwork(testNetwork)
                .withEmbeddedZookeeper()
                .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd.withName("kafka-" + seed))
                .withEnv("KAFKA_NUM_PARTITIONS", "1")
                .withStartupTimeout(Duration.ofSeconds(30));
        kafkaContainer.start();

        // oddly, container name start with a /
        internalBootstrapServers = String.format("PLAINTEXT:/%s:%s", kafkaContainer.getContainerName(), 9092);
        schemaRegistryContainer = SchemaRegistryContainer
                .create(KAFKA_SCHEMA_REGISTRY_IMAGE, seed, internalBootstrapServers)
                .withNetwork(testNetwork)
                .withStartupTimeout(Duration.ofSeconds(30));
        schemaRegistryContainer.start();
    }

    @AfterAll
    public static void closeAfterAll() {
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
    public void testProducer() throws InterruptedException, IOException {
        String buildDir = System.getProperty("buildDir");
        String projectVersion = System.getProperty("projectVersion");
        String jarFile = String.format(Locale.ROOT, "producer-v3-kafka-%s-all.jar", projectVersion);
        try (CassandraContainer<?> cassandraContainer = new CassandraContainer<>(CASSANDRA_IMAGE)
                .withConfigurationOverride("cassandra-cdc")
                .withCreateContainerCmdModifier(c -> c.withName("cassandra"))
                .withLogConsumer(new Slf4jLogConsumer(log))
                .withNetwork(testNetwork)
                .withFileSystemBind(
                        String.format(Locale.ROOT, "%s/libs/%s", buildDir, jarFile),
                        String.format(Locale.ROOT, "/%s", jarFile))
                .withEnv("JVM_EXTRA_OPTS", String.format(
                        Locale.ROOT,
                        "-javaagent:/%s=kafkaBrokers=%s,kafkaSchemaRegistryUrl=%s",
                        jarFile,
                        internalBootstrapServers,
                        schemaRegistryContainer.getRegistryUrlInDockerNetwork()))
                .withStartupTimeout(Duration.ofSeconds(120))) {
            cassandraContainer.start();

            try (CqlSession cqlSession = cassandraContainer.getCqlSession()) {
                cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS ks1 WITH replication = {'class':'SimpleStrategy','replication_factor':'1'};");
                cqlSession.execute("CREATE TABLE IF NOT EXISTS ks1.table1 (id text PRIMARY KEY, a int) WITH cdc=true");
                cqlSession.execute("INSERT INTO ks1.table1 (id, a) VALUES('1',1)");
                cqlSession.execute("INSERT INTO ks1.table1 (id, a) VALUES('2',1)");
                cqlSession.execute("INSERT INTO ks1.table1 (id, a) VALUES('3',1)");

                cqlSession.execute("CREATE TABLE IF NOT EXISTS ks1.table2 (a text, b int, c int, PRIMARY KEY(a,b)) WITH cdc=true");
                cqlSession.execute("INSERT INTO ks1.table2 (a,b,c) VALUES('1',1,1)");
                cqlSession.execute("INSERT INTO ks1.table2 (a,b,c) VALUES('2',1,1)");
                cqlSession.execute("INSERT INTO ks1.table2 (a,b,c) VALUES('3',1,1)");
            }

            // wait CL sync on disk
            Thread.sleep(11000);
            // cassandra drain to discard commitlog segments without stopping the producer
            final org.testcontainers.containers.Container.ExecResult result =
                    cassandraContainer.execInContainer("/opt/cassandra/bin/nodetool", "drain");
            assertEquals(0, result.getExitCode());
            Thread.sleep(15000);

            KafkaConsumer<byte[], MutationValue> consumer = createConsumer("test-consumer-avro-group");
            consumer.subscribe(ImmutableList.of(ProducerConfig.topicPrefix + "ks1.table1", ProducerConfig.topicPrefix + "ks1.table2"));
            int noRecordsCount = 0;
            int mutationTable1 = 1;
            int mutationTable2 = 1;

            AvroConverter keyConverter = new AvroConverter();
            keyConverter.configure(
                    ImmutableMap.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryContainer.getRegistryUrl()),
                    true);
            Schema expectedKeySchema1 = SchemaBuilder.string().optional().build();
            Schema expectedKeySchema2 = SchemaBuilder.struct()
                    .name("ks1.table2")
                    .version(1)
                    .doc(KafkaMutationSender.SCHEMA_DOC_PREFIX + "ks1.table2")
                    .field("a", SchemaBuilder.string().optional().build())
                    .field("b", SchemaBuilder.int32().optional().build())
                    .build();

            while (true) {
                final ConsumerRecords<byte[], MutationValue> consumerRecords = consumer.poll(Duration.ofMillis(100));

                if (consumerRecords.count() == 0) {
                    noRecordsCount++;
                    if (noRecordsCount > GIVE_UP) break;
                }

                for (ConsumerRecord<byte[], MutationValue> record : consumerRecords) {
                    String topicName = record.topic();
                    SchemaAndValue keySchemaAndValue = keyConverter.toConnectData(topicName, record.key());
                    System.out.println("Consumer Record: topicName=" + topicName +
                            " partition=" + record.partition() +
                            " offset=" + record.offset() +
                            " key=" + keySchemaAndValue.value() +
                            " value=" + record.value());
                    if (topicName.endsWith("table1")) {
                        assertEquals(Integer.toString(mutationTable1), keySchemaAndValue.value());
                        assertEquals(expectedKeySchema1, keySchemaAndValue.schema());
                        mutationTable1++;
                    } else if (topicName.endsWith("table2")) {
                        Struct expectedKey = new Struct(keySchemaAndValue.schema())
                                .put("a", Integer.toString(mutationTable2))
                                .put("b", 1);
                        assertEquals(expectedKey, keySchemaAndValue.value());
                        assertEquals(expectedKeySchema2, keySchemaAndValue.schema());
                        mutationTable2++;
                    }
                }
                consumer.commitSync();
            }
            assertEquals(4, mutationTable1);
            assertEquals(4, mutationTable2);
            consumer.close();
        }
    }
}
