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
package com.datastax.oss.cdc;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.testcontainers.cassandra.CassandraContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Base class for agent unit tests with a single cassandra node and Kafka.
 */
@Slf4j
public abstract class KafkaSingleNodeTests {

    private static Network testNetwork;
    private static KafkaContainer kafkaContainer;

    final AgentTestUtil.Version version;

    public KafkaSingleNodeTests(AgentTestUtil.Version version) {
        this.version = version;
    }

    public abstract CassandraContainer<?> createCassandraContainer(int nodeIndex, String kafkaBootstrapServers, Network testNetwork);

    public void drain(CassandraContainer... cassandraContainers) throws IOException, InterruptedException {
        // do nothing by default
    }

    public abstract int getSegmentSize();

    @BeforeAll
    public static void initBeforeClass() throws Exception {
        testNetwork = Network.newNetwork();
        kafkaContainer = new KafkaContainer(AgentTestUtil.KAFKA_IMAGE)
                .withNetwork(testNetwork)
                .withNetworkAliases("kafka")
                .withKraft()
                .withStartupTimeout(Duration.ofSeconds(60));
        kafkaContainer.start();
    }

    @AfterAll
    public static void closeAfterAll() {
        kafkaContainer.close();
    }

    protected KafkaConsumer<String, byte[]> createKafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return new KafkaConsumer<>(props);
    }

    @Test
    public void testBasicProducer() throws InterruptedException, IOException {
        String kafkaBootstrapServers = kafkaContainer.getBootstrapServers();
        try (CassandraContainer<?> cassandraContainer1 = createCassandraContainer(1, kafkaBootstrapServers, testNetwork)) {
            cassandraContainer1.start();
            
            try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS ks1 WITH replication = {'class':'SimpleStrategy','replication_factor':'1'};");
                cqlSession.execute("CREATE TABLE IF NOT EXISTS ks1.table1 (id text PRIMARY KEY, a int) WITH cdc=true");
                cqlSession.execute("INSERT INTO ks1.table1 (id, a) VALUES('1',1)");
                cqlSession.execute("INSERT INTO ks1.table1 (id, a) VALUES('2',2)");
                cqlSession.execute("INSERT INTO ks1.table1 (id, a) VALUES('3',3)");
            }

            drain(cassandraContainer1);

            try (KafkaConsumer<String, byte[]> consumer = createKafkaConsumer()) {
                consumer.subscribe(Collections.singletonList("events-ks1.table1"));
                
                int messageCount = 0;
                long startTime = System.currentTimeMillis();
                while (messageCount < 3 && (System.currentTimeMillis() - startTime) < 60000) {
                    ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(5));
                    for (ConsumerRecord<String, byte[]> record : records) {
                        log.info("Received Kafka message: topic={}, partition={}, offset={}, key={}",
                                record.topic(), record.partition(), record.offset(), record.key());
                        messageCount++;
                        assertNotNull(record.value(), "Message value should not be null");
                    }
                }
                
                assertEquals(3, messageCount, "Expected 3 CDC mutations in Kafka");
            }
        }
    }

    @Test
    public void testMultipleTablesProducer() throws InterruptedException, IOException {
        String kafkaBootstrapServers = kafkaContainer.getBootstrapServers();
        try (CassandraContainer<?> cassandraContainer1 = createCassandraContainer(1, kafkaBootstrapServers, testNetwork)) {
            cassandraContainer1.start();
            
            try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS ks2 WITH replication = {'class':'SimpleStrategy','replication_factor':'1'};");
                cqlSession.execute("CREATE TABLE IF NOT EXISTS ks2.table1 (id text PRIMARY KEY, a int) WITH cdc=true");
                cqlSession.execute("CREATE TABLE IF NOT EXISTS ks2.table2 (a text, b int, c int, PRIMARY KEY(a,b)) WITH cdc=true");
                
                cqlSession.execute("INSERT INTO ks2.table1 (id, a) VALUES('1',1)");
                cqlSession.execute("INSERT INTO ks2.table1 (id, a) VALUES('2',2)");
                
                cqlSession.execute("INSERT INTO ks2.table2 (a,b,c) VALUES('1',1,1)");
                cqlSession.execute("INSERT INTO ks2.table2 (a,b,c) VALUES('2',1,1)");
            }

            drain(cassandraContainer1);

            // Verify table1 messages
            try (KafkaConsumer<String, byte[]> consumer = createKafkaConsumer()) {
                consumer.subscribe(Collections.singletonList("events-ks2.table1"));
                
                int messageCount = 0;
                long startTime = System.currentTimeMillis();
                while (messageCount < 2 && (System.currentTimeMillis() - startTime) < 60000) {
                    ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(5));
                    messageCount += records.count();
                }
                
                assertEquals(2, messageCount, "Expected 2 CDC mutations for table1");
            }

            // Verify table2 messages
            try (KafkaConsumer<String, byte[]> consumer = createKafkaConsumer()) {
                consumer.subscribe(Collections.singletonList("events-ks2.table2"));
                
                int messageCount = 0;
                long startTime = System.currentTimeMillis();
                while (messageCount < 2 && (System.currentTimeMillis() - startTime) < 60000) {
                    ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(5));
                    messageCount += records.count();
                }
                
                assertEquals(2, messageCount, "Expected 2 CDC mutations for table2");
            }
        }
    }
}

