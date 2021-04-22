package com.datastax.cassandra.cdc.producer;

import com.datastax.cassandra.cdc.MutationValue;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.testcontainers.cassandra.CassandraContainer;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Locale;
import java.util.Properties;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class PulsarProducerTests {

    public static final String CASSANDRA_IMAGE = "cassandra:4.0-beta4";
    public static final String PULSAR_VERSION = "latest";

    static final String PULSAR_IMAGE = "harbor.sjc.dsinternal.org/pulsar/pulsar-all:" + PULSAR_VERSION;

    private static Network testNetwork;
    private static CassandraContainer cassandraContainer;
    private static PulsarContainer pulsarContainer;

    static final int GIVE_UP = 100;

    @BeforeAll
    public static final void initBeforeClass() throws Exception {
        testNetwork = Network.newNetwork();

        pulsarContainer = new PulsarContainer(DockerImageName.parse(PULSAR_IMAGE))
                .withNetwork(testNetwork)
                .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd.withName("pulsar"))
                .withStartupTimeout(Duration.ofSeconds(30));
        pulsarContainer.start();

        String buildDir = System.getProperty("buildDir");
        String projectVersion = System.getProperty("projectVersion");
        String jarFile = String.format(Locale.ROOT, "producer-v4-pulsar-%s-all.jar", projectVersion);
        cassandraContainer = new CassandraContainer(CASSANDRA_IMAGE)
                .withCreateContainerCmdModifier(new Consumer<CreateContainerCmd>() {
                    @Override
                    public void accept(CreateContainerCmd createContainerCmd) {
                        createContainerCmd.withName("cassandra");
                    }
                })
                .withLogConsumer(new Slf4jLogConsumer(log))
                .withNetwork(testNetwork)
                .withConfigurationOverride("cassandra-cdc")
                .withFileSystemBind(
                        String.format(Locale.ROOT, "%s/libs/%s", buildDir, jarFile),
                        String.format(Locale.ROOT, "/%s", jarFile))
                .withEnv("JVM_EXTRA_OPTS", String.format(
                        Locale.ROOT,
                        "-javaagent:/%s -pulsarServiceUrl=%s",
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
    public void testProducer() throws InterruptedException {
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
        /*
        Thread.sleep(15000);
        KafkaConsumer<byte[], MutationValue> consumer = createConsumer("test-consumer-avro-group");
        consumer.subscribe(ImmutableList.of(PropertyConfig.topicPrefix + "ks1.table1", PropertyConfig.topicPrefix + "ks1.table2"));
        int noRecordsCount = 0;
        int mutationTable1= 1;
        int mutationTable2= 1;

        AvroConverter keyConverter = new AvroConverter();
        keyConverter.configure(
                ImmutableMap.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryContainer.getRegistryUrl()),
                true);
        Schema expectedKeySchema1 = SchemaBuilder.string().optional().build();
        Schema expectedKeySchema2 = SchemaBuilder.struct()
                .name("ks1.table2")
                .version(1)
                .doc(PulsarMutationSender.SCHEMA_DOC_PREFIX + "ks1.table2")
                .field("a", SchemaBuilder.string().optional().build())
                .field("b", SchemaBuilder.int32().optional().build())
                .build();

        while(true) {
            final ConsumerRecords<byte[], MutationValue> consumerRecords = consumer.poll(Duration.ofMillis(100));

            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > GIVE_UP) break;
            }

            for (ConsumerRecord<byte[], MutationValue> record : consumerRecords) {
                String topicName = record.topic();
                SchemaAndValue keySchemaAndValue = keyConverter.toConnectData(topicName, record.key());
                System.out.println("Consumer Record: topicName=" + topicName+
                        " partition=" + record.partition() +
                        " offset=" +record.offset() +
                        " key=" + keySchemaAndValue.value() +
                        " value="+ record.value());
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
        System.out.println("noRecordsCount=" + noRecordsCount);
        assertEquals(4, mutationTable1);
        assertEquals(4, mutationTable2);
        consumer.close();

         */
    }
}
