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
import com.datastax.testcontainers.PulsarContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Base class for agent unit tests with 2 cassandra nodes.
 */
@Slf4j
public abstract class PulsarDualNodeTests {

    private static Network testNetwork;
    private static PulsarContainer<?> pulsarContainer;

    final AgentTestUtil.Version version;

    public PulsarDualNodeTests(AgentTestUtil.Version version)
    {
        this.version = version;
    }

    public abstract CassandraContainer<?> createCassandraContainer(int nodeIndex, String pulsarServiceUrl, Network testNetwork);

    public void drain(CassandraContainer... cassandraContainers) throws IOException, InterruptedException {
        // do nothing by default
    }

    @BeforeAll
    public static void initBeforeClass() throws Exception {
        testNetwork = Network.newNetwork();
        pulsarContainer = new PulsarContainer<>(AgentTestUtil.PULSAR_IMAGE)
                .withNetwork(testNetwork)
                .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd.withName("pulsar"))
                .withStartupTimeout(Duration.ofSeconds(30));
        pulsarContainer.start();
        Container.ExecResult result = pulsarContainer.execInContainer(
                "/pulsar/bin/pulsar-admin", "namespaces", "set-is-allow-auto-update-schema", "public/default", "--enable");
        assertEquals(0, result.getExitCode());
        result = pulsarContainer.execInContainer(
                "/pulsar/bin/pulsar-admin", "namespaces", "set-deduplication", "public/default", "--enable");
        assertEquals(0, result.getExitCode());
    }

    @AfterAll
    public static void closeAfterAll() {
        pulsarContainer.close();
    }

    @Test
    public void testProducer() throws InterruptedException, IOException {
        String pulsarServiceUrl = "pulsar://pulsar:" + pulsarContainer.BROKER_PORT;
        try (CassandraContainer<?> cassandraContainer1 = createCassandraContainer(1, pulsarServiceUrl, testNetwork);
             CassandraContainer<?> cassandraContainer2 = createCassandraContainer(2, pulsarServiceUrl, testNetwork)) {
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

            drain(cassandraContainer1, cassandraContainer2);

            Map<String, List<UUID>> nodesTable1 = new HashMap<>();
            Map<String, List<UUID>> nodesTable2 = new HashMap<>();
            Map<String, List<String>> digestsTable1 = new HashMap<>();
            Map<String, List<String>> digestsTable2 = new HashMap<>();

            try (PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(pulsarContainer.getPulsarBrokerUrl()).build()) {
                RecordSchemaBuilder recordSchemaBuilder1 = SchemaBuilder.record("ks1.table1");
                recordSchemaBuilder1.field("id").type(SchemaType.STRING).required();
                SchemaInfo keySchemaInfo1 = recordSchemaBuilder1.build(SchemaType.AVRO);
                Schema<GenericRecord> keySchema1 = Schema.generic(keySchemaInfo1);
                Schema<KeyValue<GenericRecord, MutationValue>> schema1 = Schema.KeyValue(
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
                            nodesTable1.values().stream().mapToInt(List::size).sum() < 6) {
                        KeyValue<GenericRecord, MutationValue> kv = msg.getValue();
                        GenericRecord key = kv.getKey();
                        MutationValue val = kv.getValue();
                        System.out.println("Consumer Record: topicName=" + msg.getTopicName() +
                                " key=" + AgentTestUtil.genericRecordToString(key) +
                                " value=" + val);
                        List<UUID> nodes = nodesTable1.computeIfAbsent((String) key.getField("id"), k -> new ArrayList<>());
                        nodes.add(val.getNodeId());
                        List<String> digests = digestsTable1.computeIfAbsent((String) key.getField("id"), k -> new ArrayList<>());
                        digests.add(val.getMd5Digest());
                        consumer.acknowledgeAsync(msg);
                    }
                }
                // check we have exactly one mutation per node for each key.
                for (int i = 1; i < 4; i++) {
                    Assert.assertNotNull(nodesTable1.get(Integer.toString(i)));
                    assertEquals(2, nodesTable1.get(Integer.toString(i)).size());
                    assertEquals(2, nodesTable1.get(Integer.toString(i)).stream().collect(Collectors.toSet()).size());
                }
                // check we have exactly 2 identical digests.
                for (int i = 1; i < 4; i++) {
                    Assert.assertNotNull(digestsTable1.get(Integer.toString(i)));
                    assertEquals(2, digestsTable1.get(Integer.toString(i)).size());
                    assertEquals(1, digestsTable1.get(Integer.toString(i)).stream().collect(Collectors.toSet()).size());
                }

                // pulsar-admin schemas get "persistent://public/default/events-ks1.table2"
                // pulsar-admin topics peek-messages persistent://public/default/events-ks1.table2-partition-0 --count 3 --subscription sub1
                RecordSchemaBuilder recordSchemaBuilder2 = SchemaBuilder.record("ks1.table2");
                recordSchemaBuilder2.field("a").type(SchemaType.STRING).required();
                recordSchemaBuilder2.field("b").type(SchemaType.INT32).optional().defaultValue(null);
                SchemaInfo keySchemaInfo2 = recordSchemaBuilder2.build(SchemaType.AVRO);
                Schema<GenericRecord> keySchema2 = Schema.generic(keySchemaInfo2);
                Schema<KeyValue<GenericRecord, MutationValue>> schema2 = Schema.KeyValue(
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
                            nodesTable2.values().stream().mapToInt(List::size).sum() < 6) {
                        KeyValue<GenericRecord, MutationValue> kv = msg.getValue();
                        GenericRecord key = kv.getKey();
                        MutationValue val = kv.getValue();
                        System.out.println("Consumer Record: topicName=" + msg.getTopicName() +
                                " key=" + AgentTestUtil.genericRecordToString(key) +
                                " value=" + val);
                        assertEquals(1, key.getField("b"));
                        List<UUID> nodes = nodesTable2.computeIfAbsent((String) key.getField("a"), k -> new ArrayList<>());
                        nodes.add(val.getNodeId());
                        List<String> digests = digestsTable2.computeIfAbsent((String) key.getField("a"), k -> new ArrayList<>());
                        digests.add(val.getMd5Digest());
                        consumer.acknowledgeAsync(msg);
                    }
                }
                // check we have exactly one mutation per node for each key.
                for (int i = 1; i < 4; i++) {
                    assertEquals(2, nodesTable2.get(Integer.toString(i)).size());
                    assertEquals(2, nodesTable2.get(Integer.toString(i)).stream().collect(Collectors.toSet()).size());
                }
                // check we have exactly 2 identical digests.
                for (int i = 1; i < 4; i++) {
                    assertEquals(2, digestsTable2.get(Integer.toString(i)).size());
                    assertEquals(1, digestsTable2.get(Integer.toString(i)).stream().collect(Collectors.toSet()).size());
                }
            }
        }
    }

    @Test
    public void testUnorderedMutations() throws InterruptedException, IOException {
        if (!version.equals(AgentTestUtil.Version.C4)) {
            log.info("Skipping this test not for agent c4");
            return;
        }

        String pulsarServiceUrl = "pulsar://pulsar:" + pulsarContainer.BROKER_PORT;
        Long testId = Math.abs(AgentTestUtil.random.nextLong());
        String randomDataDir = System.getProperty("buildDir") + "/data-" + testId + "-";
        try (CassandraContainer<?> cassandraContainer1 = createCassandraContainer(1, pulsarServiceUrl, testNetwork)
                .withFileSystemBind(randomDataDir + "1", "/var/lib/cassandra");
             CassandraContainer<?> cassandraContainer2 = createCassandraContainer(2, pulsarServiceUrl, testNetwork)
                     .withFileSystemBind(randomDataDir + "2", "/var/lib/cassandra")) {
            cassandraContainer1.start();
            cassandraContainer2.start();

            try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS ks1 WITH replication = {'class':'SimpleStrategy','replication_factor':'2'};");
                cqlSession.execute("CREATE TABLE IF NOT EXISTS ks1.table1 (id text PRIMARY KEY, a int) WITH cdc=true");
                cqlSession.execute("INSERT INTO ks1.table1 (id, a) VALUES('1',1)");
            }

            Thread.sleep(10000); // wait disk sync to avoid losing the first mutation.
            cassandraContainer2.stop();
            try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                cqlSession.execute("INSERT INTO ks1.table1 (id, a) VALUES('2',1)");
                cqlSession.execute("DELETE FROM ks1.table1 WHERE id = ?", "2");
            }
            cassandraContainer2.start();

            drain(cassandraContainer1, cassandraContainer2);

            Map<String, List<UUID>> nodesPerPk = new HashMap<>();
            Map<String, List<String>> digestsPerPk = new HashMap<>();
            Map<UUID, List<String>> digestPerNode = new HashMap<>();
            try (PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(pulsarContainer.getPulsarBrokerUrl()).build()) {
                RecordSchemaBuilder recordSchemaBuilder1 = SchemaBuilder.record("ks1.table1");
                recordSchemaBuilder1.field("id").type(SchemaType.STRING).required();
                SchemaInfo keySchemaInfo1 = recordSchemaBuilder1.build(SchemaType.AVRO);
                Schema<GenericRecord> keySchema1 = Schema.generic(keySchemaInfo1);
                Schema<KeyValue<GenericRecord, MutationValue>> schema1 = Schema.KeyValue(
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
                    while ((msg = consumer.receive(60, TimeUnit.SECONDS)) != null &&
                            nodesPerPk.values().stream().mapToInt(List::size).sum() < 6) {
                        KeyValue<GenericRecord, MutationValue> kv = msg.getValue();
                        GenericRecord key = kv.getKey();
                        MutationValue val = kv.getValue();
                        System.out.println("Consumer Record: topicName=" + msg.getTopicName() +
                                " key=" + AgentTestUtil.genericRecordToString(key) +
                                " value=" + val);
                        List<UUID> nodes = nodesPerPk.computeIfAbsent((String) key.getField("id"), k -> new ArrayList<>());
                        nodes.add(val.getNodeId());
                        List<String> digests = digestsPerPk.computeIfAbsent((String) key.getField("id"), k -> new ArrayList<>());
                        digests.add(val.getMd5Digest());
                        List<String> digest2 = digestPerNode.computeIfAbsent(val.getNodeId(), k -> new ArrayList<>());
                        digest2.add(val.getMd5Digest());
                        consumer.acknowledgeAsync(msg);
                    }
                }
                // check mutation per node.
                Assert.assertNotNull(nodesPerPk.get("1"));
                assertEquals(2, nodesPerPk.get("1").size());
                assertEquals(2, nodesPerPk.get("1").stream().collect(Collectors.toSet()).size());
                Assert.assertNotNull(nodesPerPk.get("2"));
                assertEquals(4, nodesPerPk.get("2").size());
                assertEquals(2, nodesPerPk.get("2").stream().collect(Collectors.toSet()).size());

                // check digests.
                Assert.assertNotNull(digestsPerPk.get("1"));
                assertEquals(2, digestsPerPk.get("1").size());
                assertEquals(1, digestsPerPk.get("1").stream().collect(Collectors.toSet()).size());
                Assert.assertNotNull(digestsPerPk.get("2"));
                assertEquals(4, digestsPerPk.get("2").size());
                assertEquals(2, digestsPerPk.get("2").stream().collect(Collectors.toSet()).size());

                // check mutation are not in the same order from node1 and node2 (because of replayed hint handoff)
                UUID node1 = nodesPerPk.get("1").get(0);
                UUID node2 = nodesPerPk.get("1").get(1);
                Assert.assertNotEquals(node1, node2);

                // not always in a different order, may depend on the system scheduling
                //Assert.assertNotEquals(digestPerNode.get(node1), digestPerNode.get(node2));
            }
        }
    }
}
