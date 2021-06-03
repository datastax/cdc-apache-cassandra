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
import org.apache.avro.Conversions;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
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
import org.testcontainers.shaded.org.apache.commons.lang.RandomStringUtils;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class KafkaProducerV3Tests {

    public static final DockerImageName CASSANDRA_IMAGE = DockerImageName.parse(
            Optional.ofNullable(System.getenv("CASSANDRA_IMAGE")).orElse("cassandra:3.11.10"))
            .asCompatibleSubstituteFor("cassandra");

    public static final String CONFLUENT_PLATFORM_VERSION = "5.5.1";

    public static final DockerImageName KAFKA_IMAGE = DockerImageName.parse(
            Optional.ofNullable(System.getenv("KAFKA_IMAGE"))
                    .orElse("confluentinc/cp-kafka:" + CONFLUENT_PLATFORM_VERSION))
            .asCompatibleSubstituteFor("kafka");
    public static final DockerImageName KAFKA_SCHEMA_REGISTRY_IMAGE = DockerImageName.parse(
            Optional.ofNullable(System.getenv("KAFKA_SCHEMA_REGISTRY_IMAGE"))
                    .orElse("confluentinc/cp-schema-registry:" + CONFLUENT_PLATFORM_VERSION));

    private static String seed;
    private static Network testNetwork;
    private static KafkaContainer kafkaContainer;
    private static SchemaRegistryContainer schemaRegistryContainer;
    private static String internalKafkaBootstrapServers;
    private static String schemaRegistryUrl;
    static final int GIVE_UP = 100;

    @BeforeAll
    public static final void initBeforeClass() throws Exception {
        testNetwork = Network.newNetwork();

        // seed to uniquely identify containers between concurrent tests.
        seed = RandomStringUtils.randomNumeric(6);

        kafkaContainer = new KafkaContainer(KAFKA_IMAGE)
                .withNetwork(testNetwork)
                .withEmbeddedZookeeper()
                .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd.withName("kafka-" + seed))
                .withEnv("KAFKA_NUM_PARTITIONS", "1")
                .withStartupTimeout(Duration.ofSeconds(30));
        kafkaContainer.start();

        // oddly, container name start with a /
        internalKafkaBootstrapServers = String.format("PLAINTEXT:/%s:%s", kafkaContainer.getContainerName(), 9092);
        schemaRegistryContainer = SchemaRegistryContainer
                .create(KAFKA_SCHEMA_REGISTRY_IMAGE, seed, internalKafkaBootstrapServers)
                .withNetwork(testNetwork)
                .withStartupTimeout(Duration.ofSeconds(30));
        schemaRegistryContainer.start();
        schemaRegistryUrl = schemaRegistryContainer.getRegistryUrlInDockerNetwork();
    }

    @AfterAll
    public static void closeAfterAll() {
        kafkaContainer.close();
        schemaRegistryContainer.close();
    }

    KafkaConsumer<byte[], GenericRecord> createConsumer(String groupId) {
        // Add the AVRO logical type for UUID
        ReflectData.get().addLogicalTypeConversion(new Conversions.UUIDConversion());
        ReflectData.AllowNull.get().addLogicalTypeConversion(new Conversions.UUIDConversion());

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
        try (CassandraContainer<?> cassandraContainer1 = CassandraContainer.createCassandraContainerWithKafkaProducer(
                CASSANDRA_IMAGE, testNetwork, 1, "v3", internalKafkaBootstrapServers, schemaRegistryUrl);
             CassandraContainer<?> cassandraContainer2 = CassandraContainer.createCassandraContainerWithKafkaProducer(
                     CASSANDRA_IMAGE, testNetwork, 2, "v3", internalKafkaBootstrapServers, schemaRegistryUrl)) {
            cassandraContainer1.start();
            cassandraContainer2.start();

            try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS ks1 WITH replication = {'class':'SimpleStrategy','replication_factor':'2'};");
                cqlSession.execute("CREATE TABLE IF NOT EXISTS ks1.table1 (id text PRIMARY KEY, a int) WITH cdc=true");
                cqlSession.execute("INSERT INTO ks1.table1 (id, a) VALUES('1',1)");
                cqlSession.execute("INSERT INTO ks1.table1 (id, a) VALUES('2',1)");
                cqlSession.execute("INSERT INTO ks1.table1 (id, a) VALUES('3',1)");

                cqlSession.execute("CREATE TABLE IF NOT EXISTS ks1.table2 (a text, b int, c int, PRIMARY KEY(a,b)) WITH cdc=true");
                cqlSession.execute("INSERT INTO ks1.table2 (a,b,c) VALUES('1',1,1)");
                cqlSession.execute("INSERT INTO ks1.table2 (a,b,c) VALUES('2',1,1)");
                cqlSession.execute("INSERT INTO ks1.table2 (a,b,c) VALUES('3',1,1)");
            }

            Thread.sleep(15000);    // wait CL sync on disk
            // cassandra drain to discard commitlog segments without stopping the producer
            assertEquals(0, cassandraContainer1.execInContainer("/opt/cassandra/bin/nodetool", "drain").getExitCode());
            assertEquals(0, cassandraContainer2.execInContainer("/opt/cassandra/bin/nodetool", "drain").getExitCode());
            Thread.sleep(11000);

            KafkaConsumer<byte[], GenericRecord> consumer = createConsumer("test-consumer-avro-group");
            ProducerConfig config = ProducerConfig.create(ProducerConfig.Plateform.KAFKA, null); // default config
            consumer.subscribe(ImmutableList.of(config.topicPrefix + "ks1.table1", config.topicPrefix + "ks1.table2"));
            int noRecordsCount = 0;

            Map<String, List<UUID>> nodesTable1 = new HashMap<>();
            Map<String, List<UUID>> nodesTable2 = new HashMap<>();
            Map<String, List<String>> digestsTable1 = new HashMap<>();
            Map<String, List<String>> digestsTable2 = new HashMap<>();

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
                final ConsumerRecords<byte[], GenericRecord> consumerRecords = consumer.poll(Duration.ofMillis(100));

                if (consumerRecords.count() == 0) {
                    noRecordsCount++;
                    if (noRecordsCount > GIVE_UP) break;
                }

                for (ConsumerRecord<byte[], GenericRecord> record : consumerRecords) {
                    String topicName = record.topic();
                    SchemaAndValue keySchemaAndValue = keyConverter.toConnectData(topicName, record.key());
                    GenericRecord genericRecord = record.value();
                    System.out.println("Consumer Record: topicName=" + topicName +
                            " partition=" + record.partition() +
                            " offset=" + record.offset() +
                            " key=" + keySchemaAndValue.value() +
                            " value=" + genericRecord);
                    UUID mutationNodeId = UUID.fromString(genericRecord.get("nodeId").toString());
                    String mutationMd5Digest = genericRecord.get("md5Digest").toString();
                    if (topicName.endsWith("table1")) {
                        assertEquals(expectedKeySchema1, keySchemaAndValue.schema());
                        nodesTable1.computeIfAbsent((String) keySchemaAndValue.value(), k -> new ArrayList<>()).add(mutationNodeId);
                        digestsTable1.computeIfAbsent((String) keySchemaAndValue.value(), k -> new ArrayList<>()).add(mutationMd5Digest);
                    } else if (topicName.endsWith("table2")) {
                        String key = (String) ((Struct) keySchemaAndValue.value()).get("a");
                        assertEquals(1, ((Struct) keySchemaAndValue.value()).get("b"));
                        assertEquals(expectedKeySchema2, keySchemaAndValue.schema());
                        nodesTable2.computeIfAbsent(key, k -> new ArrayList<>()).add(mutationNodeId);
                        digestsTable2.computeIfAbsent(key, k -> new ArrayList<>()).add(mutationMd5Digest);
                    }
                }
                consumer.commitSync();
            }
            // check we have exactly one mutation per node for each key.
            for (int i = 1; i < 4; i++) {
                assertEquals(2, nodesTable1.get(Integer.toString(i)).size());
                assertEquals(2, nodesTable1.get(Integer.toString(i)).stream().collect(Collectors.toSet()).size());
                assertEquals(2, nodesTable2.get(Integer.toString(i)).size());
                assertEquals(2, nodesTable2.get(Integer.toString(i)).stream().collect(Collectors.toSet()).size());
            }
            // check we have exactly 2 identical digests.
            for (int i = 1; i < 4; i++) {
                assertEquals(2, digestsTable1.get(Integer.toString(i)).size());
                assertEquals(1, digestsTable1.get(Integer.toString(i)).stream().collect(Collectors.toSet()).size());
                assertEquals(2, digestsTable2.get(Integer.toString(i)).size());
                assertEquals(1, digestsTable2.get(Integer.toString(i)).stream().collect(Collectors.toSet()).size());
            }
            consumer.close();
        }
    }
}
