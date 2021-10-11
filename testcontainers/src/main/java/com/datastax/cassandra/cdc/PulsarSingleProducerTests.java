/**
 * Copyright DataStax, Inc 2021.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.cassandra.cdc;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.testcontainers.cassandra.CassandraContainer;
import com.datastax.testcontainers.pulsar.PulsarContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.KeyValue;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.datastax.cassandra.cdc.ProducerTestUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Base class for producer unit tests with a single cassandra node.
 */
@Slf4j
public abstract class PulsarSingleProducerTests {

    private static Network testNetwork;
    private static PulsarContainer<?> pulsarContainer;

    public abstract CassandraContainer<?> createCassandraContainer(int nodeIndex, String pulsarServiceUrl, Network testNetwork);

    public void drain(CassandraContainer... cassandraContainers) throws IOException, InterruptedException {
        // do nothing by default
    }

    public static void initBeforeClass() throws Exception {
        testNetwork = Network.newNetwork();
        pulsarContainer = new PulsarContainer<>(PULSAR_IMAGE)
                .withNetwork(testNetwork)
                .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd.withName("pulsar"))
                .withStartupTimeout(Duration.ofSeconds(30));
        pulsarContainer.start();
        Container.ExecResult result = pulsarContainer.execInContainer(
                "/pulsar/bin/pulsar-admin", "namespaces", "set-is-allow-auto-update-schema", "public/default", "--enable");
        assertEquals(0, result.getExitCode());
    }

    public static void closeAfterAll() {
        pulsarContainer.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSchema() throws IOException, InterruptedException {
        final String pulsarServiceUrl = "pulsar://pulsar:" + pulsarContainer.BROKER_PORT;

        final ZoneId zone = ZoneId.systemDefault();
        final LocalDate localDate = LocalDate.of(2020, 12, 25);
        final LocalDateTime localDateTime = localDate.atTime(10, 10, 00);

        // sample values, left=CQL value, right=Pulsar value
        Map<String, Object[]> values = new HashMap<>();
        values.put("text", new Object[]{"a", "a"});
        values.put("ascii", new Object[]{"aa", "aa"});
        values.put("boolean", new Object[]{true, true});
        values.put("blob", new Object[]{ByteBuffer.wrap(new byte[]{0x00, 0x01}), ByteBuffer.wrap(new byte[]{0x00, 0x01})});
        values.put("timestamp", new Object[]{localDateTime.atZone(zone).toInstant(), localDateTime.atZone(zone).toInstant().toEpochMilli()});
        values.put("time", new Object[]{localDateTime.toLocalTime(), (localDateTime.toLocalTime().toNanoOfDay() / 1000)});
        values.put("date", new Object[]{localDateTime.toLocalDate(), (int) localDateTime.toLocalDate().toEpochDay()});
        values.put("uuid", new Object[]{UUID.fromString("01234567-0123-0123-0123-0123456789ab"), "01234567-0123-0123-0123-0123456789ab"});
        values.put("timeuuid", new Object[]{UUID.fromString("d2177dd0-eaa2-11de-a572-001b779c76e3"), "d2177dd0-eaa2-11de-a572-001b779c76e3"});
        values.put("tinyint", new Object[]{(byte) 0x01, (int) 0x01}); // Avro only support integer
        values.put("smallint", new Object[]{(short) 1, (int) 1});     // Avro only support integer
        values.put("int", new Object[]{1, 1});
        values.put("bigint", new Object[]{1L, 1L});
        values.put("double", new Object[]{1.0D, 1.0D});
        values.put("float", new Object[]{1.0f, 1.0f});
        values.put("inet4", new Object[]{Inet4Address.getLoopbackAddress(), Inet4Address.getLoopbackAddress().getHostAddress()});
        values.put("inet6", new Object[]{Inet6Address.getLoopbackAddress(), Inet4Address.getLoopbackAddress().getHostAddress()});
        values.put("varint", new Object[] {new BigInteger("314"), new CqlLogicalTypes.CqlVarintConversion().toBytes(new BigInteger("314"), CqlLogicalTypes.varintType, CqlLogicalTypes.CQL_VARINT_LOGICAL_TYPE)});
        values.put("decimal", new Object[] {new BigDecimal(314.16), new BigDecimal(314.16)});
        values.put("duration", new Object[] { CqlDuration.newInstance(1,2,3), CqlDuration.newInstance(1,2,3)});

        try (CassandraContainer<?> cassandraContainer1 = createCassandraContainer(1, pulsarServiceUrl, testNetwork)) {
            cassandraContainer1.start();
            try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS ks2 WITH replication = {'class':'SimpleStrategy','replication_factor':'1'};");
                cqlSession.execute("CREATE TABLE IF NOT EXISTS ks2.table1 (" +
                        "xtext text, xascii ascii, xboolean boolean, xblob blob, xtimestamp timestamp, xtime time, xdate date, xuuid uuid, xtimeuuid timeuuid, xtinyint tinyint, xsmallint smallint, xint int, xbigint bigint, xvarint varint, xdecimal decimal, xdouble double, xfloat float, xinet4 inet, xinet6 inet, " +
                        "primary key (xtext, xascii, xboolean, xblob, xtimestamp, xtime, xdate, xuuid, xtimeuuid, xtinyint, xsmallint, xint, xbigint, xvarint, xdecimal, xdouble, xfloat, xinet4, xinet6)) " +
                        "WITH CLUSTERING ORDER BY (xascii ASC, xboolean DESC, xblob ASC, xtimestamp DESC, xtime DESC, xdate ASC, xuuid DESC, xtimeuuid ASC, xtinyint DESC, xsmallint ASC, xint DESC, xbigint ASC, xvarint DESC, xdecimal ASC, xdouble DESC, xfloat ASC, xinet4 ASC, xinet6 DESC) AND cdc=true");
                cqlSession.execute("INSERT INTO ks2.table1 (xtext, xascii, xboolean, xblob, xtimestamp, xtime, xdate, xuuid, xtimeuuid, xtinyint, xsmallint, xint, xbigint, xvarint, xdecimal, xdouble, xfloat, xinet4, xinet6) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        values.get("text")[0],
                        values.get("ascii")[0],
                        values.get("boolean")[0],
                        values.get("blob")[0],
                        values.get("timestamp")[0],
                        values.get("time")[0],
                        values.get("date")[0],
                        values.get("uuid")[0],
                        values.get("timeuuid")[0],
                        values.get("tinyint")[0],
                        values.get("smallint")[0],
                        values.get("int")[0],
                        values.get("bigint")[0],
                        values.get("varint")[0],
                        values.get("decimal")[0],
                        values.get("double")[0],
                        values.get("float")[0],
                        values.get("inet4")[0],
                        values.get("inet6")[0]
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
                Assert.assertNotNull("Expecting one message, check the producer log", msg);
                GenericRecord gr = msg.getValue();
                KeyValue<GenericRecord, GenericRecord> kv = (KeyValue<GenericRecord, GenericRecord>) gr.getNativeObject();
                GenericRecord key = kv.getKey();
                System.out.println("Consumer Record: topicName=" + msg.getTopicName() + " key=" + genericRecordToString(key));
                Map<String, Object> keyMap = genericRecordToMap(key);
                for (Field field : key.getFields()) {
                    String vKey = field.getName().substring(1);
                    Assert.assertTrue("Unknown field " + vKey, values.containsKey(vKey));
                    if (keyMap.get(field.getName()) instanceof GenericRecord) {
                        assertGenericRecords(vKey, (GenericRecord) keyMap.get(field.getName()), values);
                    } else {
                        Assert.assertEquals("Wrong value for PK field " + field.getName(),
                                values.get(vKey)[1],
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
                Assert.assertNotNull("Expecting one message, check the producer log", msg);
                GenericRecord gr = msg.getValue();
                KeyValue<GenericRecord, GenericRecord> kv = (KeyValue<GenericRecord, GenericRecord>) gr.getNativeObject();
                GenericRecord key = kv.getKey();
                Assert.assertEquals("a", key.getField("a"));
                Assert.assertEquals("b", key.getField("b"));
                consumer.acknowledgeAsync(msg);

                msg = consumer.receive(90, TimeUnit.SECONDS);
                Assert.assertNotNull("Expecting one message, check the producer log", msg);
                GenericRecord gr2 = msg.getValue();
                KeyValue<GenericRecord, GenericRecord> kv2 = (KeyValue<GenericRecord, GenericRecord>) gr2.getNativeObject();
                GenericRecord key2 = kv2.getKey();
                Assert.assertEquals("a", key2.getField("a"));
                Assert.assertEquals(null, key2.getField("b"));
                consumer.acknowledgeAsync(msg);

                msg = consumer.receive(90, TimeUnit.SECONDS);
                Assert.assertNotNull("Expecting one message, check the producer log", msg);
                GenericRecord gr3 = msg.getValue();
                KeyValue<GenericRecord, GenericRecord> kv3 = (KeyValue<GenericRecord, GenericRecord>) gr3.getNativeObject();
                GenericRecord key3 = kv3.getKey();
                Assert.assertEquals("a", key3.getField("a"));
                Assert.assertEquals(null, key3.getField("b"));
                consumer.acknowledgeAsync(msg);
            }
        }
    }
}
