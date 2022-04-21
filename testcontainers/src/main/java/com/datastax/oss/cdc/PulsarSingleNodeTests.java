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
import com.datastax.testcontainers.ChaosNetworkContainer;
import com.datastax.testcontainers.PulsarContainer;
import com.datastax.testcontainers.cassandra.CassandraContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.KeyValue;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.datastax.oss.cdc.DataSpec.dataSpecMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Base class for agent unit tests with a single cassandra node.
 */
@Slf4j
public abstract class PulsarSingleNodeTests {

    private static Network testNetwork;
    private static PulsarContainer<?> pulsarContainer;

    final AgentTestUtil.Version version;

    public PulsarSingleNodeTests(AgentTestUtil.Version version) {
        this.version = version;
    }

    public abstract CassandraContainer<?> createCassandraContainer(int nodeIndex, String pulsarServiceUrl, Network testNetwork);

    public void drain(CassandraContainer... cassandraContainers) throws IOException, InterruptedException {
        // do nothing by default
    }

    public abstract int getSegmentSize();

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

        // disable delete inactive topics
        Container.ExecResult result2 = pulsarContainer.execInContainer(
                "/pulsar/bin/pulsar-admin", "namespaces", "set-inactive-topic-policies", "--disable-delete-while-inactive", "-t", "2d", "-m", "delete_when_subscriptions_caught_up", "public/default");
        assertEquals(0, result2.getExitCode(), "set-inactive-topic-policies failed:" + result2.getStdout());

        // enable message deduplication
        Container.ExecResult result3 = pulsarContainer.execInContainer(
                "/pulsar/bin/pulsar-admin", "namespaces", "set-deduplication", "public/default", "--enable");
        assertEquals(0, result3.getExitCode(), "set-deduplication failed:" + result2.getStdout());
    }

    @AfterAll
    public static void closeAfterAll() {
        pulsarContainer.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSchema() throws IOException, InterruptedException {
        final String pulsarServiceUrl = "pulsar://pulsar:" + pulsarContainer.BROKER_PORT;
        try (CassandraContainer<?> cassandraContainer1 = createCassandraContainer(1, pulsarServiceUrl, testNetwork)) {
            cassandraContainer1.start();
            try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS ks2 WITH replication = {'class':'SimpleStrategy','replication_factor':'1'};");
                cqlSession.execute("CREATE TABLE IF NOT EXISTS ks2.table1 (" +
                        "xtext text, xascii ascii, xboolean boolean, xblob blob, xtimestamp timestamp, xtime time, xdate date, xuuid uuid, xtimeuuid timeuuid, xtinyint tinyint, xsmallint smallint, xint int, xbigint bigint, xvarint varint, xdecimal decimal, xdouble double, xfloat float, xinet4 inet, xinet6 inet, " +
                        "primary key (xtext, xascii, xboolean, xblob, xtimestamp, xtime, xdate, xuuid, xtimeuuid, xtinyint, xsmallint, xint, xbigint, xvarint, xdecimal, xdouble, xfloat, xinet4, xinet6)) " +
                        "WITH CLUSTERING ORDER BY (xascii ASC, xboolean DESC, xblob ASC, xtimestamp DESC, xtime DESC, xdate ASC, xuuid DESC, xtimeuuid ASC, xtinyint DESC, xsmallint ASC, xint DESC, xbigint ASC, xvarint DESC, xdecimal ASC, xdouble DESC, xfloat ASC, xinet4 ASC, xinet6 DESC) AND cdc=true");
                cqlSession.execute("INSERT INTO ks2.table1 (xtext, xascii, xboolean, xblob, xtimestamp, xtime, xdate, xuuid, xtimeuuid, xtinyint, xsmallint, xint, xbigint, xvarint, xdecimal, xdouble, xfloat, xinet4, xinet6) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        dataSpecMap.get("text").cqlValue,
                        dataSpecMap.get("ascii").cqlValue,
                        dataSpecMap.get("boolean").cqlValue,
                        dataSpecMap.get("blob").cqlValue,
                        dataSpecMap.get("timestamp").cqlValue,
                        dataSpecMap.get("time").cqlValue,
                        dataSpecMap.get("date").cqlValue,
                        dataSpecMap.get("uuid").cqlValue,
                        dataSpecMap.get("timeuuid").cqlValue,
                        dataSpecMap.get("tinyint").cqlValue,
                        dataSpecMap.get("smallint").cqlValue,
                        dataSpecMap.get("int").cqlValue,
                        dataSpecMap.get("bigint").cqlValue,
                        dataSpecMap.get("varint").cqlValue,
                        dataSpecMap.get("decimal").cqlValue,
                        dataSpecMap.get("double").cqlValue,
                        dataSpecMap.get("float").cqlValue,
                        dataSpecMap.get("inet4").cqlValue,
                        dataSpecMap.get("inet6").cqlValue
                );
            }

            drain(cassandraContainer1);

            try (PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(pulsarContainer.getPulsarBrokerUrl()).build();
                 Consumer<GenericRecord> consumer = pulsarClient.newConsumer(Schema.AUTO_CONSUME())
                         .topic("events-ks2.table1")
                         .subscriptionName("sub1")
                         .subscriptionType(SubscriptionType.Key_Shared)
                         .subscriptionMode(SubscriptionMode.Durable)
                         .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                         .subscribe()) {
                Message<GenericRecord> msg = consumer.receive(60, TimeUnit.SECONDS);
                Assert.assertNotNull("Expecting one message, check the agent log", msg);
                GenericRecord gr = msg.getValue();
                KeyValue<GenericRecord, GenericRecord> kv = (KeyValue<GenericRecord, GenericRecord>) gr.getNativeObject();
                GenericRecord key = kv.getKey();
                System.out.println("Consumer Record: topicName=" + msg.getTopicName() + " key=" + AgentTestUtil.genericRecordToString(key));
                Map<String, Object> keyMap = AgentTestUtil.genericRecordToMap(key);
                for (Field field : key.getFields()) {
                    String vKey = field.getName().substring(1);
                    Assert.assertTrue("Unknown field " + vKey, dataSpecMap.containsKey(vKey));
                    if (keyMap.get(field.getName()) instanceof GenericRecord) {
                        AgentTestUtil.assertGenericRecords(vKey, (GenericRecord) keyMap.get(field.getName()), dataSpecMap);
                    } else {
                        Assert.assertEquals("Wrong value for PK field " + field.getName(),
                                dataSpecMap.get(vKey).avroValue,
                                keyMap.get(field.getName()));
                    }
                }
                consumer.acknowledgeAsync(msg);
            }
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testStaticColumn() throws IOException, InterruptedException {
        String pulsarServiceUrl = "pulsar://pulsar:" + pulsarContainer.BROKER_PORT;
        try (CassandraContainer<?> cassandraContainer1 = createCassandraContainer(1, pulsarServiceUrl, testNetwork)) {
            cassandraContainer1.start();
            try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS ks3 WITH replication = {'class':'SimpleStrategy','replication_factor':'1'};");
                cqlSession.execute("CREATE TABLE IF NOT EXISTS ks3.table1 (a text, b text, c text, d text static, PRIMARY KEY ((a), b)) with cdc=true;");
                cqlSession.execute("INSERT INTO ks3.table1 (a,b,c,d) VALUES ('a','b','c','d1');");
                cqlSession.execute("INSERT INTO ks3.table1 (a,d) VALUES ('a','d2');");
                cqlSession.execute("DELETE FROM ks3.table1 WHERE a = 'a'");
            }

            drain(cassandraContainer1);

            try (PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(pulsarContainer.getPulsarBrokerUrl()).build();
                 Consumer<GenericRecord> consumer = pulsarClient.newConsumer(Schema.AUTO_CONSUME())
                         .topic("events-ks3.table1")
                         .subscriptionName("sub1")
                         .subscriptionType(SubscriptionType.Key_Shared)
                         .subscriptionMode(SubscriptionMode.Durable)
                         .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                         .subscribe()) {
                Message<GenericRecord> msg = consumer.receive(120, TimeUnit.SECONDS);
                Assert.assertNotNull("Expecting one message, check the agent log", msg);
                GenericRecord gr = msg.getValue();
                KeyValue<GenericRecord, GenericRecord> kv = (KeyValue<GenericRecord, GenericRecord>) gr.getNativeObject();
                GenericRecord key = kv.getKey();
                Assert.assertEquals("a", key.getField("a"));
                Assert.assertEquals("b", key.getField("b"));
                consumer.acknowledgeAsync(msg);

                msg = consumer.receive(90, TimeUnit.SECONDS);
                Assert.assertNotNull("Expecting one message, check the agent log", msg);
                GenericRecord gr2 = msg.getValue();
                KeyValue<GenericRecord, GenericRecord> kv2 = (KeyValue<GenericRecord, GenericRecord>) gr2.getNativeObject();
                GenericRecord key2 = kv2.getKey();
                Assert.assertEquals("a", key2.getField("a"));
                Assert.assertEquals(null, key2.getField("b"));
                consumer.acknowledgeAsync(msg);

                msg = consumer.receive(90, TimeUnit.SECONDS);
                Assert.assertNotNull("Expecting one message, check the agent log", msg);
                GenericRecord gr3 = msg.getValue();
                KeyValue<GenericRecord, GenericRecord> kv3 = (KeyValue<GenericRecord, GenericRecord>) gr3.getNativeObject();
                GenericRecord key3 = kv3.getKey();
                Assert.assertEquals("a", key3.getField("a"));
                Assert.assertEquals(null, key3.getField("b"));
                consumer.acknowledgeAsync(msg);
            }
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testMultithreadProcessing() throws IOException, InterruptedException {
        String pulsarServiceUrl = "pulsar://pulsar:" + pulsarContainer.BROKER_PORT;
        int numMutation = 200;

        try (CassandraContainer<?> cassandraContainer1 = createCassandraContainer(1, pulsarServiceUrl, testNetwork)) {
            cassandraContainer1.start();
            Executors.newSingleThreadExecutor().submit(() -> {
                try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                    cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS mt WITH replication = {'class':'SimpleStrategy','replication_factor':'1'};");
                    cqlSession.execute("CREATE TABLE IF NOT EXISTS mt.table1 (a int, b blob, PRIMARY KEY (a)) with cdc=true;");
                    for (int i = 0; i < numMutation; i++) {
                        cqlSession.execute("INSERT INTO mt.table1 (a,b) VALUES (?, ?);", i, AgentTestUtil.randomizeBuffer(getSegmentSize() / 4));
                        Thread.sleep(631);
                    }
                    if (version.equals(AgentTestUtil.Version.C3)) {
                        // fill up the last CL file and flush for Cassandra 3.11
                        cqlSession.execute("CREATE TABLE IF NOT EXISTS mt.table2 (a int, b blob, PRIMARY KEY (a)) with cdc=false;");
                        for (int i = 0; i < 8; i++) {
                            cqlSession.execute("INSERT INTO mt.table2 (a,b) VALUES (?, ?);", i, AgentTestUtil.randomizeBuffer(getSegmentSize() / 4));
                        }
                        Thread.sleep(11000); // wait for sync
                        Container.ExecResult flushResult = cassandraContainer1.execInContainer("/opt/cassandra/bin/nodetool", "flush");
                        assertEquals(0, flushResult.getExitCode(), "nodetool flush error:" + flushResult.getStdout());
                    }
                } catch (Exception e) {
                    log.error("error:", e);
                }
            });

            int msgCount = 0;
            long maxLatency = 0;
            List<String> segAndPos = new ArrayList<>(numMutation);
            try (PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(pulsarContainer.getPulsarBrokerUrl()).build();
                 Consumer<GenericRecord> consumer = pulsarClient.newConsumer(Schema.AUTO_CONSUME())
                         .topic("events-mt.table1")
                         .subscriptionName("sub1")
                         .subscriptionType(SubscriptionType.Key_Shared)
                         .subscriptionMode(SubscriptionMode.Durable)
                         .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                         .subscribe()) {
                Message<GenericRecord> msg;
                while ((msg = consumer.receive(120, TimeUnit.SECONDS)) != null) {
                    Assert.assertNotNull("Expecting one message, check the agent log", msg);
                    msgCount++;
                    String segpos = msg.getProperty(Constants.SEGMENT_AND_POSITION);
                    assertFalse(segAndPos.contains(segpos), "Already received mutation position=" + segpos+" positions=" + segAndPos);
                    segAndPos.add(segpos);

                    long writetime = Long.parseLong(msg.getProperty(Constants.WRITETIME));
                    long now = System.currentTimeMillis();
                    long latency = now * 1000 - writetime;
                    maxLatency = Math.max(maxLatency, latency);
                    consumer.acknowledgeAsync(msg);
                }
            }
            assertEquals(numMutation, segAndPos.size(), "Unexpected segAndPos=" + segAndPos);
            assertEquals(numMutation, msgCount, "Unexpected msgCount=" + msgCount);

            assertTrue(maxLatency > 0);
            if (!version.equals(AgentTestUtil.Version.C3))
                assertTrue(maxLatency <= 20000000);

            Container.ExecResult result = cassandraContainer1.execInContainer("ls", "-1", "/var/lib/cassandra/cdc");
            String[] files = result.getStdout().split("\\n");
            assertTrue(files.length <= 3);
            for(String f : files)
                assertTrue( f.endsWith("_offset.dat") || f.equals("archives") || f.equals("errors"));

            Container.ExecResult result2 = cassandraContainer1.execInContainer("ls", "-1", "/var/lib/cassandra/cdc_raw");
            String[] files2 = result2.getStdout().split("\\n");
            assertTrue(files2.length <= 3);
            for(String f : files2)
                if (f.length() > 0) // cdc_raw may be empty
                    assertTrue( f.endsWith("_cdc.idx") || f.endsWith(".log"));

            if (version.equals(AgentTestUtil.Version.DSE4)) {
                Container.ExecResult sentMutations = cassandraContainer1.execInContainer("nodetool", "sjk", "mx", "-b", "org.apache.cassandra.metrics:name=SentMutations,type=CdcAgent","-f", "Count", "-mg");
                String[] sentMutationLines = sentMutations.getStdout().split("\\n");
                assertEquals(numMutation, Long.parseLong(sentMutationLines[1]));

                Container.ExecResult maxSubmittedTasks = cassandraContainer1.execInContainer("nodetool", "sjk", "mx", "-b", "org.apache.cassandra.metrics:name=MaxSubmittedTasks,type=CdcAgent","-f", "Value", "-mg");
                String[] maxSubmittedTasksLines = maxSubmittedTasks.getStdout().split("\\n");
                assertTrue(Long.parseLong(maxSubmittedTasksLines[1]) > 0);

                Container.ExecResult maxPendingTasks = cassandraContainer1.execInContainer("nodetool", "sjk", "mx", "-b", "org.apache.cassandra.metrics:name=MaxPendingTasks,type=CdcAgent","-f", "Value", "-mg");
                String[] maxPendingTasksLines = maxPendingTasks.getStdout().split("\\n");
                assertTrue(Long.parseLong(maxPendingTasksLines[1]) > 0);

                Container.ExecResult submittedTasks = cassandraContainer1.execInContainer("nodetool", "sjk", "mx", "-b", "org.apache.cassandra.metrics:name=SubmittedTasks,type=CdcAgent","-f", "Value", "-mg");
                String[] submittedTasksLines = submittedTasks.getStdout().split("\\n");
                assertEquals(0, Long.parseLong(submittedTasksLines[1]));

                Container.ExecResult pendingTasks = cassandraContainer1.execInContainer("nodetool", "sjk", "mx", "-b", "org.apache.cassandra.metrics:name=PendingTasks,type=CdcAgent","-f", "Value", "-mg");
                String[] pendingTasksLines = pendingTasks.getStdout().split("\\n");
                assertEquals(0, Long.parseLong(pendingTasksLines[1]));

                Container.ExecResult executedTasks = cassandraContainer1.execInContainer("nodetool", "sjk", "mx", "-b", "org.apache.cassandra.metrics:name=ExecutedTasks,type=CdcAgent","-f", "Count", "-mg");
                String[] executedTasksLines = executedTasks.getStdout().split("\\n");
                assertTrue(Long.parseLong(executedTasksLines[1]) < numMutation);

                Container.ExecResult uncleanedTasks = cassandraContainer1.execInContainer("nodetool", "sjk", "mx", "-b", "org.apache.cassandra.metrics:name=UncleanedTasks,type=CdcAgent","-f", "Value", "-mg");
                String[] uncleanedTasksLines = uncleanedTasks.getStdout().split("\\n");
                assertEquals(0, Long.parseLong(uncleanedTasksLines[1]));
            }
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testNearRealTimeProcessing() throws IOException, InterruptedException {
        if (version.equals(AgentTestUtil.Version.C3)) {
            log.info("Skipping this test for agent c3");
            return;
        }

        String pulsarServiceUrl = "pulsar://pulsar:" + pulsarContainer.BROKER_PORT;
        try (CassandraContainer<?> cassandraContainer1 = createCassandraContainer(1, pulsarServiceUrl, testNetwork)) {
            cassandraContainer1.start();
            try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS nrt WITH replication = {'class':'SimpleStrategy','replication_factor':'1'};");
                cqlSession.execute("CREATE TABLE IF NOT EXISTS nrt.table1 (a int, b blob, PRIMARY KEY (a)) with cdc=true;");
                cqlSession.execute("INSERT INTO nrt.table1 (a,b) VALUES (?, ?);", 1, AgentTestUtil.randomizeBuffer(1));
            }

            final int numMutation = 10;
            int i = 1;
            List<String> segAndPos = new ArrayList<>();
            try (PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(pulsarContainer.getPulsarBrokerUrl()).build();
                 Consumer<GenericRecord> consumer = pulsarClient.newConsumer(Schema.AUTO_CONSUME())
                         .topic("events-nrt.table1")
                         .subscriptionName("sub1")
                         .subscriptionType(SubscriptionType.Key_Shared)
                         .subscriptionMode(SubscriptionMode.Durable)
                         .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                         .subscribe()) {
                Message<GenericRecord> msg;
                while ((msg = consumer.receive(90, TimeUnit.SECONDS)) != null && i < numMutation) {
                    Assert.assertNotNull("Expecting one message, check the agent log", msg);
                    String segpos = msg.getProperty(Constants.SEGMENT_AND_POSITION);
                    assertFalse(segAndPos.contains(segpos), "Already received mutation position=" + segpos+" positions=" + segAndPos);
                    segAndPos.add(segpos);

                    i++;
                    try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                        cqlSession.execute("INSERT INTO nrt.table1 (a,b) VALUES (?, ?);", i, AgentTestUtil.randomizeBuffer(i));
                    }
                    consumer.acknowledge(msg);
                }
            }
            assertEquals(i, numMutation);

            if (version.equals(AgentTestUtil.Version.DSE4)) {
                Container.ExecResult sentMutations = cassandraContainer1.execInContainer("nodetool", "sjk", "mx", "-b", "org.apache.cassandra.metrics:name=SentMutations,type=CdcAgent","-f", "Count", "-mg");
                String[] sentMutationLines = sentMutations.getStdout().split("\\n");
                assertEquals(numMutation, Long.parseLong(sentMutationLines[1]), "nodetool failed:" + sentMutations.getStdout());

                Container.ExecResult maxSubmittedTasks = cassandraContainer1.execInContainer("nodetool", "sjk", "mx", "-b", "org.apache.cassandra.metrics:name=MaxSubmittedTasks,type=CdcAgent","-f", "Value", "-mg");
                String[] maxSubmittedTasksLines = maxSubmittedTasks.getStdout().split("\\n");
                assertEquals(1, Long.parseLong(maxSubmittedTasksLines[1]), "nodetool failed:" + maxSubmittedTasks.getStdout());

                Container.ExecResult executedTasks = cassandraContainer1.execInContainer("nodetool", "sjk", "mx", "-b", "org.apache.cassandra.metrics:name=ExecutedTasks,type=CdcAgent","-f", "Count", "-mg");
                String[] executedTasksLines = executedTasks.getStdout().split("\\n");
                assertEquals(numMutation, Long.parseLong(executedTasksLines[1]), "nodetool failed:" + executedTasks.getStdout());

                Container.ExecResult uncleanedTasks = cassandraContainer1.execInContainer("nodetool", "sjk", "mx", "-b", "org.apache.cassandra.metrics:name=UncleanedTasks,type=CdcAgent","-f", "Value", "-mg");
                String[] uncleanedTasksLines = uncleanedTasks.getStdout().split("\\n");
                assertEquals(0, Long.parseLong(uncleanedTasksLines[1]), "nodetool failed:" + uncleanedTasks.getStdout());

                Container.ExecResult maxUncleanedTasks = cassandraContainer1.execInContainer("nodetool", "sjk", "mx", "-b", "org.apache.cassandra.metrics:name=MaxUncleanedTasks,type=CdcAgent","-f", "Value", "-mg");
                String[] maxUncleanedTasksLines = maxUncleanedTasks.getStdout().split("\\n");
                assertTrue(Long.parseLong(maxUncleanedTasksLines[1]) <= 1, "nodetool failed:" + maxUncleanedTasks.getStdout());
            }
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testInvalidSchema() throws IOException, InterruptedException {
        if (version.equals(AgentTestUtil.Version.C3)) {
            log.info("Skipping this test for agent c3");
            return;
        }

        String pulsarServiceUrl = "pulsar://pulsar:" + pulsarContainer.BROKER_PORT;
        try (CassandraContainer<?> cassandraContainer1 = createCassandraContainer(1, pulsarServiceUrl, testNetwork)) {
            cassandraContainer1.start();
            try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS cr WITH replication = {'class':'SimpleStrategy','replication_factor':'1'};");
                cqlSession.execute("CREATE TABLE IF NOT EXISTS cr.table1 (a int, b blob, PRIMARY KEY (a)) with cdc=true;");
                cqlSession.execute("INSERT INTO cr.table1 (a,b) VALUES (?, ?);", 1, AgentTestUtil.randomizeBuffer(1));
                cqlSession.execute("DROP TABLE cr.table1");
            }

            try (PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(pulsarContainer.getPulsarBrokerUrl()).build();
                 Consumer<GenericRecord> consumer = pulsarClient.newConsumer(Schema.AUTO_CONSUME())
                         .topic("events-cr.table1")
                         .subscriptionName("sub1")
                         .subscriptionType(SubscriptionType.Key_Shared)
                         .subscriptionMode(SubscriptionMode.Durable)
                         .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                         .subscribe()) {

                Message<GenericRecord> msg = consumer.receive(60, TimeUnit.SECONDS);
                Assert.assertNotNull("Expecting one message, check the agent log", msg);
                consumer.acknowledge(msg);

                // re-create the same table with a different schema
                try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                    cqlSession.execute("CREATE TABLE IF NOT EXISTS cr.table1 (b int, c blob, PRIMARY KEY (b)) with cdc=true;");
                    cqlSession.execute("INSERT INTO cr.table1 (b,c) VALUES (?, ?);", 2, AgentTestUtil.randomizeBuffer(1));
                }

                Message<GenericRecord> msg2 = consumer.receive(30, TimeUnit.SECONDS);
                Assert.assertNull("Expecting no message, check the agent log", msg2);
                if (version.equals(AgentTestUtil.Version.DSE4)) {
                    Container.ExecResult skippedMutations = cassandraContainer1.execInContainer("nodetool", "sjk", "mx", "-b", "org.apache.cassandra.metrics:name=SkippedMutations,type=CdcAgent", "-f", "Count", "-mg");
                    String[] skippedMutationsLines = skippedMutations.getStdout().split("\\n");
                    assertEquals(1L, Long.parseLong(skippedMutationsLines[1]), "skippedMutations failed:" + skippedMutations.getStdout());
                }
            }
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testPulsarReconnection() throws IOException, InterruptedException {
        if (version.equals(AgentTestUtil.Version.C3)) {
            log.info("Skipping this test for agent c3");
            return;
        }
        String pulsarServiceUrl = "pulsar://pulsar:" + pulsarContainer.BROKER_PORT;
        int numMutation = 100;

        try (CassandraContainer<?> cassandraContainer1 = createCassandraContainer(1, pulsarServiceUrl, testNetwork)) {
            cassandraContainer1.start();
            try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS pulsarfailure WITH replication = {'class':'SimpleStrategy','replication_factor':'1'};");
                cqlSession.execute("CREATE TABLE IF NOT EXISTS pulsarfailure.table1 (a int, b blob, PRIMARY KEY (a)) with cdc=true;");
                for (int i = 0; i < numMutation; i++) {
                    cqlSession.execute("INSERT INTO pulsarfailure.table1 (a,b) VALUES (?, ?);", i, AgentTestUtil.randomizeBuffer(getSegmentSize() / 4));
                }
            }

            try (CqlSession cqlSession = cassandraContainer1.getCqlSession();
                    ChaosNetworkContainer<?> chaosContainer =
                            new ChaosNetworkContainer<>(pulsarContainer.getContainerName(), "100s");) {

                chaosContainer.start();
                // write 100 mutations during 100s (pulsar request timeout is 60s)
                for (int i = 0; i < numMutation; i++) {
                    cqlSession.execute("INSERT INTO pulsarfailure.table1 (a,b) VALUES (?, ?);", 2 * i, AgentTestUtil.randomizeBuffer(getSegmentSize() / 4));
                    Thread.sleep(1000);
                }
            }

            // wait the end of the network outage.
            Thread.sleep(1000);

            int msgCount = 0;
            long maxLatency = 0;
            try (PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(pulsarContainer.getPulsarBrokerUrl()).build();
                 Consumer<GenericRecord> consumer = pulsarClient.newConsumer(Schema.AUTO_CONSUME())
                         .topic("events-pulsarfailure.table1")
                         .subscriptionName("sub1")
                         .subscriptionType(SubscriptionType.Key_Shared)
                         .subscriptionMode(SubscriptionMode.Durable)
                         .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                         .subscribe()) {
                Message<GenericRecord> msg;
                while ((msg = consumer.receive(240, TimeUnit.SECONDS)) != null && msgCount < 2 * numMutation) {
                    Assert.assertNotNull("Expecting one message, check the agent log", msg);
                    msgCount++;
                    consumer.acknowledgeAsync(msg);

                    long writetime = Long.parseLong(msg.getProperty(Constants.WRITETIME));
                    long now = System.currentTimeMillis();
                    long latency = now * 1000 - writetime;
                    maxLatency = Math.max(maxLatency, latency);
                    consumer.acknowledgeAsync(msg);
                }
                assertEquals(2 * numMutation, msgCount);
                assertTrue(maxLatency > 0);
            }
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testPartitionedTopic() throws IOException, InterruptedException {
        final String pulsarServiceUrl = "pulsar://pulsar:" + pulsarContainer.BROKER_PORT;

        Container.ExecResult result = pulsarContainer.execInContainer(
                "/pulsar/bin/pulsar-admin", "topics", "create-partitioned-topic",
                "persistent://public/default/events-pt.table1", "--partitions", "3");
        assertEquals(0, result.getExitCode(), "create-partitioned-topic failed:" + result.getStdout());

        int numMutation = 100;
        try (CassandraContainer<?> cassandraContainer1 = createCassandraContainer(1, pulsarServiceUrl, testNetwork)) {
            cassandraContainer1.start();
            try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS pt WITH replication = {'class':'SimpleStrategy','replication_factor':'1'};");
                cqlSession.execute("CREATE TABLE IF NOT EXISTS pt.table1 (a int, b blob, PRIMARY KEY (a)) with cdc=true;");
                for (int i = 0; i < 100; i++)
                    cqlSession.execute("INSERT INTO pt.table1 (a,b) VALUES (?, ?);", i, AgentTestUtil.randomizeBuffer(i));
            }
            drain(cassandraContainer1);
            try (PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(pulsarContainer.getPulsarBrokerUrl()).build();
                 Consumer<GenericRecord> consumer = pulsarClient.newConsumer(Schema.AUTO_CONSUME())
                         .topic("events-pt.table1")
                         .subscriptionName("sub1")
                         .subscriptionType(SubscriptionType.Key_Shared)
                         .subscriptionMode(SubscriptionMode.Durable)
                         .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                         .subscribe()) {
                Message msg;
                int numMessage = 0;
                while ((msg = consumer.receive(60, TimeUnit.SECONDS)) != null && numMessage < numMutation) {
                    numMessage++;
                    consumer.acknowledge(msg);
                }
                assertEquals(numMutation, numMessage);
            }
        }
    }
}
