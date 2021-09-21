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
package com.datastax.cassandra.cdc.producer;

import com.datastax.cassandra.cdc.MutationValue;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.testcontainers.cassandra.CassandraContainer;
import com.datastax.testcontainers.pulsar.PulsarContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.cassandra.utils.Pair;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.nio.ByteBuffer;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class PulsarProducerV4Tests {

    public static final DockerImageName CASSANDRA_IMAGE = DockerImageName.parse(
            Optional.ofNullable(System.getenv("CASSANDRA_IMAGE"))
                    .orElse("cassandra:4.0-beta4")
    ).asCompatibleSubstituteFor("cassandra");

    public static final DockerImageName PULSAR_IMAGE = DockerImageName.parse(
            Optional.ofNullable(System.getenv("PULSAR_IMAGE"))
                    .orElse("harbor.sjc.dsinternal.org/pulsar/lunastreaming-all:latest-272")
    ).asCompatibleSubstituteFor("pulsar");

    private static Network testNetwork;
    private static PulsarContainer<?> pulsarContainer;

    @BeforeAll
    public static final void initBeforeClass() throws Exception {
        testNetwork = Network.newNetwork();
        pulsarContainer = new PulsarContainer<>(PULSAR_IMAGE)
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
        try (CassandraContainer<?> cassandraContainer1 = CassandraContainer.createCassandraContainerWithPulsarProducer(
                CASSANDRA_IMAGE, testNetwork, 1, "v4", pulsarServiceUrl);
             CassandraContainer<?> cassandraContainer2 = CassandraContainer.createCassandraContainerWithPulsarProducer(
                     CASSANDRA_IMAGE, testNetwork, 2, "v4", pulsarServiceUrl)) {
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

            Map<String, List<UUID>> nodesTable1 = new HashMap<>();
            Map<String, List<UUID>> nodesTable2 = new HashMap<>();
            Map<String, List<String>> digestsTable1 = new HashMap<>();
            Map<String, List<String>> digestsTable2 = new HashMap<>();

            try (PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(pulsarContainer.getPulsarBrokerUrl()).build()) {
                RecordSchemaBuilder recordSchemaBuilder1 = SchemaBuilder.record("ks1.table1");
                recordSchemaBuilder1.field("id").type(SchemaType.STRING).optional().defaultValue(null);
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
                                " key=" + genericRecordToString(key) +
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
                    assertEquals(2, nodesTable1.get(Integer.toString(i)).size());
                    assertEquals(2, nodesTable1.get(Integer.toString(i)).stream().collect(Collectors.toSet()).size());
                }
                // check we have exactly 2 identical digests.
                for (int i = 1; i < 4; i++) {
                    assertEquals(2, digestsTable1.get(Integer.toString(i)).size());
                    assertEquals(1, digestsTable1.get(Integer.toString(i)).stream().collect(Collectors.toSet()).size());
                }

                // pulsar-admin schemas get "persistent://public/default/events-ks1.table2"
                // pulsar-admin topics peek-messages persistent://public/default/events-ks1.table2-partition-0 --count 3 --subscription sub1
                RecordSchemaBuilder recordSchemaBuilder2 = SchemaBuilder.record("ks1.table2");
                recordSchemaBuilder2.field("a").type(SchemaType.STRING).optional().defaultValue(null);
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
                                " key=" + genericRecordToString(key) +
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

    static String genericRecordToString(GenericRecord genericRecord) {
        StringBuilder sb = new StringBuilder("{");
        for (Field field : genericRecord.getFields()) {
            if (sb.length() > 1)
                sb.append(",");
            sb.append(field.getName()).append("=");
            if (genericRecord.getField(field) instanceof GenericRecord) {
                sb.append(genericRecordToString((GenericRecord) genericRecord.getField(field)));
            } else {
                sb.append(genericRecord.getField(field) == null ? "null" : genericRecord.getField(field).toString());
            }
        }
        return sb.append("}").toString();
    }

    static Map<String, Object> genericRecordToMap(GenericRecord genericRecord) {
        Map<String, Object> map = new HashMap<>();
        for (Field field : genericRecord.getFields()) {
            if (genericRecord.getField(field) instanceof GenericRecord) {
                map.put(field.getName(), genericRecordToString((GenericRecord) genericRecord.getField(field)));
            } else {
                map.put(field.getName(), genericRecord.getField(field));
            }
        }
        return map;
    }

    @Test
    public void testSchema() throws IOException, InterruptedException {
        final String pulsarServiceUrl = "pulsar://pulsar:" + pulsarContainer.BROKER_PORT;

        final ZoneId zone = ZoneId.systemDefault();
        final LocalDate localDate = LocalDate.of(2020, 12, 25);
        final LocalDateTime localDateTime = localDate.atTime(10, 10, 00);

        // sample values, left=CQL value, right=Pulsar value
        Map<String, Pair<Object, Object>> values = new HashMap<>();
        values.put("xtext", Pair.create("a", "a"));
        values.put("xascii", Pair.create("aa", "aa"));
        values.put("xboolean", Pair.create(true, true));
        values.put("xblob", Pair.create(ByteBuffer.wrap(new byte[]{0x00, 0x01}), ByteBuffer.wrap(new byte[]{0x00, 0x01})));
        values.put("xtimestamp", Pair.create(localDateTime.atZone(zone).toInstant(), localDateTime.atZone(zone).toInstant().toEpochMilli()));
        values.put("xtime", Pair.create(localDateTime.toLocalTime(), (int) (localDateTime.toLocalTime().toNanoOfDay() / 1000000)));
        values.put("xdate", Pair.create(localDateTime.toLocalDate(), (int) localDateTime.toLocalDate().toEpochDay()));
        values.put("xuuid", Pair.create(UUID.fromString("01234567-0123-0123-0123-0123456789ab"), "01234567-0123-0123-0123-0123456789ab"));
        values.put("xtimeuuid", Pair.create(UUID.fromString("d2177dd0-eaa2-11de-a572-001b779c76e3"), "d2177dd0-eaa2-11de-a572-001b779c76e3"));
        values.put("xtinyint", Pair.create((byte) 0x01, (int) 0x01)); // Avro only support integer
        values.put("xsmallint", Pair.create((short) 1, (int) 1));     // Avro only support integer
        values.put("xint", Pair.create(1, 1));
        values.put("xbigint", Pair.create(1L, 1L));
        values.put("xdouble", Pair.create(1.0D, 1.0D));
        values.put("xfloat", Pair.create(1.0f, 1.0f));
        values.put("xinet4", Pair.create(Inet4Address.getLoopbackAddress(), Inet4Address.getLoopbackAddress().getHostAddress()));
        values.put("xinet6", Pair.create(Inet6Address.getLoopbackAddress(), Inet4Address.getLoopbackAddress().getHostAddress()));

        try (CassandraContainer<?> cassandraContainer1 = CassandraContainer.createCassandraContainerWithPulsarProducer(
                CASSANDRA_IMAGE, testNetwork, 1, "v4", pulsarServiceUrl)) {
            cassandraContainer1.start();
            try (CqlSession cqlSession = cassandraContainer1.getCqlSession()) {
                cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS ks2 WITH replication = {'class':'SimpleStrategy','replication_factor':'1'};");
                cqlSession.execute("CREATE TABLE IF NOT EXISTS ks2.table1 (" +
                        "xtext text, xascii ascii, xboolean boolean, xblob blob, xtimestamp timestamp, xtime time, xdate date, xuuid uuid, xtimeuuid timeuuid, xtinyint tinyint, xsmallint smallint, xint int, xbigint bigint, xdouble double, xfloat float, xinet4 inet, xinet6 inet, " +
                        "primary key (xtext, xascii, xboolean, xblob, xtimestamp, xtime, xdate, xuuid, xtimeuuid, xtinyint, xsmallint, xint, xbigint, xdouble, xfloat, xinet4, xinet6)) WITH cdc=true");
                cqlSession.execute("INSERT INTO ks2.table1 (xtext, xascii, xboolean, xblob, xtimestamp, xtime, xdate, xuuid, xtimeuuid, xtinyint, xsmallint, xint, xbigint, xdouble, xfloat, xinet4, xinet6) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        values.get("xtext").left,
                        values.get("xascii").left,
                        values.get("xboolean").left,
                        values.get("xblob").left,
                        values.get("xtimestamp").left,
                        values.get("xtime").left,
                        values.get("xdate").left,
                        values.get("xuuid").left,
                        values.get("xtimeuuid").left,
                        values.get("xtinyint").left,
                        values.get("xsmallint").left,
                        values.get("xint").left,
                        values.get("xbigint").left,
                        values.get("xdouble").left,
                        values.get("xfloat").left,
                        values.get("xinet4").left,
                        values.get("xinet6").left
                );
            }
            Thread.sleep(11000);    // wait CL sync on disk

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
                Map<String, Object> map = genericRecordToMap(key);
                for (Field field : key.getFields()) {
                    Assert.assertEquals("Wrong value fo field " + field.getName(), values.get(field.getName()).right, map.get(field.getName()));
                }
                consumer.acknowledgeAsync(msg);
            }
        }
    }
}
