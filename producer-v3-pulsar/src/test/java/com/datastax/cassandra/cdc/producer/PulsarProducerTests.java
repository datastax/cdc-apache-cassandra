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
package com.datastax.cassandra.cdc.producer;

import com.datastax.cassandra.cdc.MutationValue;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.testcontainers.cassandra.CassandraContainer;
import com.datastax.testcontainers.pulsar.PulsarContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.client.impl.schema.KeyValueSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericSchemaImpl;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class PulsarProducerTests {

    public static final String CASSANDRA_IMAGE = "cassandra:3.11.10";
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

    CassandraContainer createCassandraContainer(int nodeIndex) {
        String buildDir = System.getProperty("buildDir");
        String projectVersion = System.getProperty("projectVersion");
        String jarFile = String.format(Locale.ROOT, "producer-v3-pulsar-%s-all.jar", projectVersion);
        CassandraContainer<?> cassandraContainer = new CassandraContainer<>(CASSANDRA_IMAGE)
                .withCreateContainerCmdModifier(c -> c.withName("cassandra-" + nodeIndex))
                .withNetwork(testNetwork)
                .withConfigurationOverride("cassandra-cdc" + nodeIndex)
                .withFileSystemBind(
                        String.format(Locale.ROOT, "%s/libs/%s", buildDir, jarFile),
                        String.format(Locale.ROOT, "/%s", jarFile))
                .withEnv("JVM_EXTRA_OPTS", String.format(
                        Locale.ROOT,
                        "-javaagent:/%s=pulsarServiceUrl=%s",
                        jarFile,
                        "pulsar://pulsar:" + pulsarContainer.BROKER_PORT))
                .withStartupTimeout(Duration.ofSeconds(70));
        if (nodeIndex > 1) {
            cassandraContainer.withEnv("CASSANDRA_SEEDS","cassandra-1");
        }
        return cassandraContainer;
    }

    @Test
    public void testProducer() throws InterruptedException, IOException {
        Container.ExecResult result = pulsarContainer.execInContainer(
                "/pulsar/bin/pulsar-admin", "namespaces", "set-is-allow-auto-update-schema", "public/default", "--enable");
        assertEquals(0, result.getExitCode());
        result = pulsarContainer.execInContainer(
                "/pulsar/bin/pulsar-admin", "namespaces", "set-deduplication", "public/default", "--enable");
        assertEquals(0, result.getExitCode());

        String projectVersion = System.getProperty("projectVersion");
        try (CassandraContainer<?> cassandraContainer1 = createCassandraContainer(1);
                CassandraContainer<?> cassandraContainer2 = createCassandraContainer(2)) {
            cassandraContainer1.start();
            cassandraContainer2.start();

            try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS ks1 WITH replication = \n" +
                        "{'class':'SimpleStrategy','replication_factor':'2'};");
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

            Map<String,Integer> mutationTable1 = new HashMap<>();
            Map<String,Integer> mutationTable2 = new HashMap<>();

            try (PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(pulsarContainer.getPulsarBrokerUrl()).build()) {
                RecordSchemaBuilder recordSchemaBuilder1 = SchemaBuilder.record("ks1.table1");
                recordSchemaBuilder1.field("id").type(SchemaType.STRING).optional().defaultValue(null);
                SchemaInfo keySchemaInfo1 = recordSchemaBuilder1.build(SchemaType.AVRO);
                Schema<GenericRecord> keySchema1 = GenericSchemaImpl.of(keySchemaInfo1);
                Schema<KeyValue<GenericRecord, MutationValue>> schema1 = KeyValueSchema.of(
                        keySchema1,
                        Schema.AVRO(MutationValue.class),
                        KeyValueEncodingType.SEPARATED);

                // pulsar-admin schemas get "persistent://public/default/events-ks1.table1"
                // pulsar-admin topics peek-messages persistent://public/default/events-ks1.table1-partition-0 --count 3 --subscription sub1
                try (Consumer<KeyValue<GenericRecord, MutationValue>> consumer = pulsarClient.newConsumer(schema1)
                        .topic("events-ks1.table1")
                        .subscriptionName("sub1")
                        .subscriptionType(SubscriptionType.Key_Shared)
                        .subscriptionMode(SubscriptionMode.Durable)
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                        .subscribe()) {
                    Message<KeyValue<GenericRecord, MutationValue>> msg;
                    while ((msg = consumer.receive(30, TimeUnit.SECONDS)) != null &&
                            mutationTable1.values().stream().mapToInt(Integer::intValue).sum() < 6) {
                        KeyValue<GenericRecord, MutationValue> kv = msg.getValue();
                        GenericRecord key = kv.getKey();
                        MutationValue val = kv.getValue();
                        System.out.println("Consumer Record: topicName=" + msg.getTopicName() +
                                " key=" + genericRecordToString(key) +
                                " value=" + val);
                        mutationTable1.compute((String) key.getField("id"), (k,v) -> {
                            if (v == null)
                                v = 0;
                            return v+1;
                        });
                        consumer.acknowledgeAsync(msg);
                    }
                }
                assertEquals(2, (int) mutationTable1.get("1"));
                assertEquals(2, (int) mutationTable1.get("2"));
                assertEquals(2, (int) mutationTable1.get("3"));

                // pulsar-admin schemas get "persistent://public/default/events-ks1.table2"
                // pulsar-admin topics peek-messages persistent://public/default/events-ks1.table2-partition-0 --count 3 --subscription sub1
                RecordSchemaBuilder recordSchemaBuilder2 = SchemaBuilder.record("ks1.table2");
                recordSchemaBuilder2.field("a").type(SchemaType.STRING).optional().defaultValue(null);
                recordSchemaBuilder2.field("b").type(SchemaType.INT32).optional().defaultValue(null);
                SchemaInfo keySchemaInfo2 = recordSchemaBuilder2.build(SchemaType.AVRO);
                Schema<GenericRecord> keySchema2 = GenericSchemaImpl.of(keySchemaInfo2);
                Schema<KeyValue<GenericRecord, MutationValue>> schema2 = KeyValueSchema.of(
                        keySchema2,
                        Schema.AVRO(MutationValue.class),
                        KeyValueEncodingType.SEPARATED);
                try (Consumer<KeyValue<GenericRecord, MutationValue>> consumer = pulsarClient.newConsumer(schema2)
                        .topic("events-ks1.table2")
                        .subscriptionName("sub1")
                        .subscriptionType(SubscriptionType.Key_Shared)
                        .subscriptionMode(SubscriptionMode.Durable)
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                        .subscribe()) {
                    Message<KeyValue<GenericRecord, MutationValue>> msg;
                    while ((msg = consumer.receive(30, TimeUnit.SECONDS)) != null &&
                            mutationTable2.values().stream().mapToInt(Integer::intValue).sum() < 6) {
                        KeyValue<GenericRecord, MutationValue> kv = msg.getValue();
                        GenericRecord key = kv.getKey();
                        MutationValue val = kv.getValue();
                        System.out.println("Consumer Record: topicName=" + msg.getTopicName() +
                                " key=" + genericRecordToString(key) +
                                " value=" + val);
                        assertEquals(1, key.getField("b"));
                        mutationTable2.compute((String)key.getField("a"), (k,v) -> {
                            if (v == null)
                                v = 0;
                            return v+1;
                        });
                        consumer.acknowledgeAsync(msg);
                    }
                }
                assertEquals(2, (int) mutationTable2.get("1"));
                assertEquals(2, (int) mutationTable2.get("2"));
                assertEquals(2, (int) mutationTable2.get("3"));
            }
        }
    }

    static String genericRecordToString(GenericRecord genericRecord) {
        StringBuilder sb = new StringBuilder("{");
        for(Field field : genericRecord.getFields()) {
            if (sb.length() > 1)
                sb.append(",");
            sb.append(field.getName()).append("=");
            if (genericRecord.getField(field) instanceof GenericRecord) {
                sb.append(genericRecordToString((GenericRecord)genericRecord.getField(field)));
            } else {
                sb.append(genericRecord.getField(field).toString());
            }
        }
        return sb.append("}").toString();
    }
}
