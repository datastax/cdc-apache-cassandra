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
package com.datastax.oss.cdc.agent;

import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the parts of {@link AbstractKafkaMutationSender} the per-Cassandra-version senders inherit
 * unchanged: CQL to Avro key mapping, the unsupported-key check and the producer properties.
 */
class KafkaMutationSenderDefaultsTest {

    /** Uses the inherited type mapping; column types come from a map keyed by column name. */
    static class DefaultsSender extends AbstractKafkaMutationSender<Map<String, String>> {
        DefaultsSender(AgentConfig config, boolean useMurmur3Partitioner) {
            super(config, useMurmur3Partitioner);
        }

        @Override
        public String columnCql3Type(Map<String, String> tableMetadata, String columnName) {
            return tableMetadata.get(columnName);
        }

        @Override
        public void incSkippedMutations() {
            // not counted in this test
        }

        @Override
        public UUID getHostId() {
            return UUID.fromString("00000000-0000-0000-0000-000000000002");
        }
    }

    /** A mutation whose table metadata is the column-name to CQL-type map. */
    static class MapMutation extends AbstractMutation<Map<String, String>> {
        private final List<ColumnInfo> pkColumns;

        MapMutation(Map<String, String> columnTypes, Object[] pkValues, List<ColumnInfo> pkColumns) {
            super(UUID.fromString("00000000-0000-0000-0000-000000000002"), 1L, 0, pkValues, -1L, "digest", columnTypes, -1L);
            this.pkColumns = pkColumns;
        }

        @Override public String key() { return "ks.tbl"; }
        @Override public String name() { return "tbl"; }
        @Override public String keyspace() { return "ks"; }
        @Override public List<ColumnInfo> primaryKeyColumns() { return pkColumns; }
    }

    private static AgentConfig kafkaConfig(String args) {
        return AgentConfig.create(AgentConfig.Platform.KAFKA, args);
    }

    @Test
    void mapsCqlTypesThroughCqlAvroTypes() {
        DefaultsSender sender = new DefaultsSender(kafkaConfig("topicPrefix=events-"), false);
        Map<String, String> table = new HashMap<>();
        table.put("ts", "timestamp");

        assertEquals(Schema.Type.INT, sender.getNativeSchema("int").getType());
        assertNull(sender.getNativeSchema("list<int>"));
        assertEquals(1234L, sender.cqlToAvro(table, "ts", new Date(1234L)));
    }

    @Test
    void rejectsMutationWithUnsupportedKeyColumn() {
        DefaultsSender sender = new DefaultsSender(kafkaConfig("topicPrefix=events-"), false);
        Map<String, String> table = new HashMap<>();
        table.put("id", "text");
        table.put("tags", "frozen<list<int>>");
        List<ColumnInfo> pk = Arrays.asList(
                new KafkaMutationSenderTest.SimpleColumnInfo("id", "text", false),
                new KafkaMutationSenderTest.SimpleColumnInfo("tags", "frozen<list<int>>", true));

        assertFalse(sender.isSupported(new MapMutation(table, new Object[]{"a", null}, pk)));
    }

    @Test
    void sendsSupportedMutationAndCachesItsKeySchema() throws Exception {
        DefaultsSender sender = new DefaultsSender(kafkaConfig("topicPrefix=events-"), false);
        MockProducer<byte[], byte[]> producer = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
        sender.producer = producer;
        sender.pendingSemaphore = new Semaphore(10);
        Map<String, String> table = new HashMap<>();
        table.put("id", "text");
        table.put("ts", "timestamp");
        List<ColumnInfo> pk = Arrays.asList(
                new KafkaMutationSenderTest.SimpleColumnInfo("id", "text", false),
                new KafkaMutationSenderTest.SimpleColumnInfo("ts", "timestamp", true));
        MapMutation mutation = new MapMutation(table, new Object[]{"a", new Date(5L)}, pk);

        assertTrue(sender.isSupported(mutation));
        sender.sendMutationAsync(mutation).get();

        assertEquals(1, producer.history().size());
        assertNotNull(sender.getPkSchema("ks.tbl"));
        // Once the key schema is cached, the check short-circuits.
        assertTrue(sender.isSupported(mutation));
        assertSame(sender.getPkSchema("ks.tbl"), sender.getPkSchema("ks.tbl"));
    }

    @Test
    void buildsProducerPropertiesFromConfigFile(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("kafka.properties");
        Files.write(file, String.join("\n",
                "bootstrapServers=broker1:9093",
                "batchDelayInMs=5",
                "securityProtocol=SASL_SSL",
                "sslKeystoreLocation=/ks.jks",
                "sslKeystorePassword=kspass",
                "sslTruststoreLocation=/ts.jks",
                "sslTruststorePassword=tspass",
                "saslMechanism=PLAIN",
                "saslJaasConfig=jaas",
                "maxPendingMessages=7",
                "acks=all").getBytes(StandardCharsets.UTF_8));
        DefaultsSender sender = new DefaultsSender(
                kafkaConfig("topicPrefix=events-,kafkaConfigFile=" + file.toAbsolutePath()), true);

        Properties props = sender.buildProducerProperties(
                kafkaConfig("topicPrefix=events-,kafkaConfigFile=" + file.toAbsolutePath()));

        assertEquals("broker1:9093", props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(5L, props.get(ProducerConfig.LINGER_MS_CONFIG));
        assertEquals("SASL_SSL", props.get("security.protocol"));
        assertEquals("/ks.jks", props.get("ssl.keystore.location"));
        assertEquals("kspass", props.get("ssl.keystore.password"));
        assertEquals("/ts.jks", props.get("ssl.truststore.location"));
        assertEquals("tspass", props.get("ssl.truststore.password"));
        assertEquals("PLAIN", props.get("sasl.mechanism"));
        assertEquals("jaas", props.get("sasl.jaas.config"));
        assertEquals("all", props.get("acks"));
        assertEquals(Murmur3KafkaPartitioner.class.getName(), props.get(ProducerConfig.PARTITIONER_CLASS_CONFIG));
        assertEquals(7, sender.pendingSemaphore.availablePermits());
    }

    @Test
    void sslPasswordsFallBackToInlineAgentSettings() {
        DefaultsSender sender = new DefaultsSender(kafkaConfig("topicPrefix=events-"), false);

        Properties props = sender.buildProducerProperties(
                kafkaConfig("topicPrefix=events-,sslTruststorePassword=inline-ts,sslKeystorePassword=inline-ks"));

        assertEquals("inline-ts", props.get("ssl.truststore.password"));
        assertEquals("inline-ks", props.get("ssl.keystore.password"));
    }

    @Test
    void parsesStringValuedSettingsAndTreatsMissingRateAsUnlimited() {
        // A config whose values come back as strings, and with no rate limit value at all.
        AgentConfig config = Mockito.mock(AgentConfig.class);
        Mockito.when(config.propertyKeys()).thenReturn(Collections.emptyList());
        Mockito.when(config.get("batchDelayInMs")).thenReturn("25");
        Mockito.when(config.get("maxPendingMessages")).thenReturn("3");
        DefaultsSender sender = new DefaultsSender(kafkaConfig("topicPrefix=events-"), false);

        Properties props = sender.buildProducerProperties(config);

        assertEquals(25L, props.get(ProducerConfig.LINGER_MS_CONFIG));
        assertEquals(3, sender.pendingSemaphore.availablePermits());
        assertNull(sender.sendRateLimiter);
    }

    @Test
    void parsesStringValuedRateLimit() {
        AgentConfig config = Mockito.mock(AgentConfig.class);
        Mockito.when(config.propertyKeys()).thenReturn(Collections.emptyList());
        Mockito.when(config.get("kafkaMutationSendRateLimit")).thenReturn("50.5");
        DefaultsSender sender = new DefaultsSender(kafkaConfig("topicPrefix=events-"), false);

        sender.buildProducerProperties(config);

        assertNotNull(sender.sendRateLimiter);
        assertEquals(50.5, sender.sendRateLimiter.getRate(), 0.001);
    }

    @Test
    void waitsForAPendingPermitBeforeSending() throws Exception {
        DefaultsSender sender = new DefaultsSender(kafkaConfig("topicPrefix=events-"), false);
        MockProducer<byte[], byte[]> producer = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
        sender.producer = producer;
        sender.pendingSemaphore = new Semaphore(0);
        Map<String, String> table = new HashMap<>();
        table.put("id", "text");
        List<ColumnInfo> pk = Collections.singletonList(new KafkaMutationSenderTest.SimpleColumnInfo("id", "text", false));
        MapMutation mutation = new MapMutation(table, new Object[]{"a"}, pk);

        CompletableFuture<?> sent = CompletableFuture.supplyAsync(() -> sender.sendMutationAsync(mutation))
                .thenCompose(f -> f);
        long deadline = System.currentTimeMillis() + 10_000;
        while (sender.pendingSemaphoreRetries.get() == 0 && System.currentTimeMillis() < deadline) {
            Thread.yield();
        }
        assertTrue(sender.pendingSemaphoreRetries.get() > 0);
        assertTrue(producer.history().isEmpty());

        sender.pendingSemaphore.release();
        sent.get(10, TimeUnit.SECONDS);

        assertEquals(1, producer.history().size());
        assertEquals(1, sender.pendingSemaphore.availablePermits());
    }

    @Test
    void createsTheProducerOnFirstSend(@TempDir Path dir) throws Exception {
        // Nothing listens on port 1: the send can only fail, but the producer is created first.
        Path file = dir.resolve("kafka.properties");
        Files.write(file, String.join("\n",
                "bootstrapServers=127.0.0.1:1",
                "max.block.ms=200").getBytes(StandardCharsets.UTF_8));
        DefaultsSender sender = new DefaultsSender(
                kafkaConfig("topicPrefix=events-,kafkaConfigFile=" + file.toAbsolutePath()), false);
        Map<String, String> table = new HashMap<>();
        table.put("id", "text");
        List<ColumnInfo> pk = Collections.singletonList(new KafkaMutationSenderTest.SimpleColumnInfo("id", "text", false));
        try {
            CompletableFuture<?> sent = sender.sendMutationAsync(new MapMutation(table, new Object[]{"a"}, pk));

            assertNotNull(sender.producer);
            ExecutionException e = assertThrows(ExecutionException.class, () -> sent.get(30, TimeUnit.SECONDS));
            assertNotNull(e.getCause());
        } finally {
            sender.close();
        }
    }

    @Test
    void zeroBatchDelayLeavesLingerUnset(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("kafka.properties");
        Files.write(file, "batchDelayInMs=0\n".getBytes(StandardCharsets.UTF_8));
        DefaultsSender sender = new DefaultsSender(kafkaConfig("topicPrefix=events-"), false);

        Properties props = sender.buildProducerProperties(
                kafkaConfig("topicPrefix=events-,kafkaConfigFile=" + file.toAbsolutePath()));

        assertEquals("localhost:9092", props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertNull(props.get(ProducerConfig.LINGER_MS_CONFIG));
        assertNull(props.get(ProducerConfig.PARTITIONER_CLASS_CONFIG));
        assertEquals(1000, sender.pendingSemaphore.availablePermits());
    }
}
