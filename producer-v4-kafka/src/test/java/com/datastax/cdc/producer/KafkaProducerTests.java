package com.datastax.cdc.producer;

import com.datastax.cassandra.cdc.EnhancedKafkaContainer;
import com.datastax.cassandra.cdc.SchemaRegistryContainer;
import com.datastax.cassandra.cdc.producer.PropertyConfig;
import com.datastax.oss.driver.api.core.CqlSession;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Locale;
import java.util.Properties;

@Slf4j
public class KafkaProducerTests {

    static final String CASSANDRA_IMAGE = "cassandra:4.0-beta4";

    private static Network testNetwork;
    private static CassandraContainer cassandraContainer;
    private static EnhancedKafkaContainer kafkaContainer;
    private static SchemaRegistryContainer schemaRegistryContainer;

    @BeforeAll
    public static final void initBeforeClass() throws Exception {
        testNetwork = Network.newNetwork();
        kafkaContainer = EnhancedKafkaContainer.create();
        kafkaContainer.withNetwork(testNetwork);

        schemaRegistryContainer = SchemaRegistryContainer.create(
                kafkaContainer.getBootstrapServers())
                .withNetwork(testNetwork);

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
                        kafkaContainer.getBootstrapServersHostPort(),
                        schemaRegistryContainer.getRegistryAddressInDockerNetwork()));

        kafkaContainer.start();
        kafkaContainer.withStartupTimeout(Duration.ofSeconds(15));

        schemaRegistryContainer.start();
        schemaRegistryContainer.withStartupTimeout(Duration.ofSeconds(15));

        cassandraContainer.start();
        cassandraContainer.withStartupTimeout(Duration.ofSeconds(70));
    }

    @AfterAll
    public static void closeAfterClass() {
       cassandraContainer.close();
       kafkaContainer.close();
       schemaRegistryContainer.close();
    }

    @Test
    public void testStringPk() throws InterruptedException {
        try(CqlSession cqlSession = cassandraContainer.getCqlSession()) {
            cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS ks1 WITH replication = \n" +
                    "{'class':'SimpleStrategy','replication_factor':'1'};");
            cqlSession.execute("CREATE TABLE IF NOT EXISTS ks1.table1 (id text PRIMARY KEY, a int) WITH cdc=true");
            cqlSession.execute("INSERT INTO ks1.table1 (id, a) VALUES('1',1)");
        }
        Thread.sleep(15000);
        Properties props = new Properties();
        String bootstrapServers = kafkaContainer.getHost() + ":" +kafkaContainer.getMappedPort(kafkaContainer.getPort());
        System.out.println("bootstrapServersHostPort=" + bootstrapServers);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArrayDeserializer.class.getName());
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton(PropertyConfig.topicPrefix + "ks1.table1"));
        for (ConsumerRecord<String, byte[]> record : consumer.poll(Duration.ofMillis(20000))) {
            System.out.println("key="+record.key());
        }
    }

}
