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

import com.datastax.oss.cdc.Constants;
import com.datastax.oss.cdc.MutationValue;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;

import static org.junit.jupiter.api.Assertions.*;

public class KafkaMutationSenderTest {

    static final Schema STRING_SCHEMA = Schema.create(Schema.Type.STRING);

    MockProducer<byte[], byte[]> mockProducer;
    TestKafkaMutationSender sender;
    AgentConfig config;

    @BeforeEach
    void setUp() {
        // auto-complete = true: send() calls succeed immediately without a broker
        mockProducer = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
        config = AgentConfig.create(AgentConfig.Platform.KAFKA, "topicPrefix=events-");
        sender = new TestKafkaMutationSender(config);
        // inject the mock so initialize() is never called (no real broker needed)
        sender.producer = mockProducer;
        sender.pendingSemaphore = new Semaphore(1000);
    }

    // -------------------------------------------------------------------------
    // sendMutationAsync — happy path
    // -------------------------------------------------------------------------

    @Test
    void send_recordPublishedToCorrectTopic() throws Exception {
        CompletableFuture<?> f = sender.sendMutationAsync(mutation("ks", "tbl", "pk1", 1L, 0));
        f.get();

        assertEquals(1, mockProducer.history().size());
        assertEquals("events-ks.tbl", mockProducer.history().get(0).topic());
    }

    @Test
    void send_headersContainSegposAndToken() throws Exception {
        sender.sendMutationAsync(mutation("ks", "tbl", "pk1", 7L, 3)).get();

        ProducerRecord<byte[], byte[]> rec = mockProducer.history().get(0);
        assertEquals("7:3",  header(rec, Constants.SEGMENT_AND_POSITION));
        assertEquals("-1",   header(rec, Constants.TOKEN));   // token from TestMutation
    }

    @Test
    void send_writetimeHeaderPresentWhenTsSet() throws Exception {
        TestMutation m = mutation("ks", "tbl", "pk1", 1L, 0);
        m.tsOverride = 99_000L;

        sender.sendMutationAsync(m).get();

        assertEquals("99000", header(mockProducer.history().get(0), Constants.WRITETIME));
    }

    @Test
    void send_writetimeHeaderAbsentWhenTsIsMinusOne() throws Exception {
        sender.sendMutationAsync(mutation("ks", "tbl", "pk1", 1L, 0)).get();

        assertNull(mockProducer.history().get(0).headers().lastHeader(Constants.WRITETIME));
    }

    @Test
    void send_keyBytesNonEmpty() throws Exception {
        sender.sendMutationAsync(mutation("ks", "tbl", "pk1", 1L, 0)).get();

        byte[] key = mockProducer.history().get(0).key();
        assertNotNull(key);
        assertTrue(key.length > 0);
    }

    @Test
    void send_valueBytesNonEmpty() throws Exception {
        sender.sendMutationAsync(mutation("ks", "tbl", "pk1", 1L, 0)).get();

        byte[] value = mockProducer.history().get(0).value();
        assertNotNull(value);
        assertTrue(value.length > 0);
    }

    // -------------------------------------------------------------------------
    // sendMutationAsync — unsupported mutation
    // -------------------------------------------------------------------------

    @Test
    void send_skipsUnsupportedMutation() throws Exception {
        sender.supportedOverride = false;

        CompletableFuture<?> f = sender.sendMutationAsync(mutation("ks", "tbl", "pk1", 1L, 0));

        assertNull(f.get());
        assertTrue(mockProducer.history().isEmpty());
        assertEquals(1, sender.skippedCount);
    }

    // -------------------------------------------------------------------------
    // sendMutationAsync — producer error
    // -------------------------------------------------------------------------

    @Test
    void send_futureFailsOnProducerError() {
        mockProducer = new MockProducer<>(false, new ByteArraySerializer(), new ByteArraySerializer());
        sender.producer = mockProducer;

        CompletableFuture<?> f = sender.sendMutationAsync(mutation("ks", "tbl", "pk1", 1L, 0));
        mockProducer.errorNext(new RuntimeException("broker unavailable"));

        assertThrows(Exception.class, f::get);
    }

    // -------------------------------------------------------------------------
    // serializeMutationValue — Avro round-trip
    // -------------------------------------------------------------------------

    @Test
    void serializeMutationValue_roundTrip() throws Exception {
        UUID nodeId = UUID.randomUUID();
        MutationValue mv = new MutationValue("digest", nodeId, new String[]{"col1"});

        byte[] bytes = sender.serializeMutationValue(mv);

        GenericDatumReader<GenericRecord> reader =
                new GenericDatumReader<>(AbstractKafkaMutationSender.MUTATION_VALUE_SCHEMA);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        GenericRecord decoded = reader.read(null, decoder);

        assertEquals("digest", decoded.get("md5Digest").toString());
        assertEquals(nodeId.toString(), decoded.get("nodeId").toString());
        assertEquals("[col1]", decoded.get("columns").toString());
    }

    // -------------------------------------------------------------------------
    // topicName
    // -------------------------------------------------------------------------

    @Test
    void topicName_usesPrefixAndKey() {
        assertEquals("events-ks.tbl", sender.topicName(mutation("ks", "tbl", "pk1", 1L, 0)));
    }

    // -------------------------------------------------------------------------
    // close
    // -------------------------------------------------------------------------

    @Test
    void close_shutsDownProducer() {
        sender.close();
        assertTrue(mockProducer.closed());
        assertNull(sender.producer);
    }

    @Test
    void close_idempotentWhenProducerNull() {
        sender.producer = null;
        assertDoesNotThrow(() -> sender.close());
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static String header(ProducerRecord<byte[], byte[]> rec, String key) {
        Header h = rec.headers().lastHeader(key);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    private static TestMutation mutation(String ks, String tbl, Object pkValue,
                                         long segment, int position) {
        return new TestMutation(ks, tbl, new Object[]{pkValue}, segment, position);
    }

    // -------------------------------------------------------------------------
    // Minimal in-module concrete implementations (no Cassandra dependency)
    // -------------------------------------------------------------------------

    static class TestKafkaMutationSender extends AbstractKafkaMutationSender<String> {
        boolean supportedOverride = true;
        int skippedCount = 0;

        TestKafkaMutationSender(AgentConfig config) {
            super(config, false);
        }

        @Override public Schema getNativeSchema(String cql3Type) { return STRING_SCHEMA; }
        @Override public Object cqlToAvro(String meta, String col, Object value) { return value; }
        @Override public boolean isSupported(AbstractMutation<String> m) { return supportedOverride; }
        @Override public void incSkippedMutations() { skippedCount++; }
        @Override public UUID getHostId() { return UUID.fromString("00000000-0000-0000-0000-000000000001"); }
    }

    static class TestMutation extends AbstractMutation<String> {
        long tsOverride = -1L;

        TestMutation(String ks, String tbl, Object[] pkValues, long segment, int position) {
            super(UUID.fromString("00000000-0000-0000-0000-000000000001"),
                    segment, position, pkValues, -1L, "digest", tbl, -1L);
            this.ks = ks; this.tbl = tbl;
        }

        private final String ks, tbl;
        private final List<ColumnInfo> pkCols =
                Collections.singletonList(new SimpleColumnInfo("id", "varchar", false));

        @Override public long getTs() { return tsOverride; }
        @Override public String key() { return ks + "." + tbl; }
        @Override public String name() { return tbl; }
        @Override public String keyspace() { return ks; }
        @Override public List<ColumnInfo> primaryKeyColumns() { return pkCols; }
    }

    static class SimpleColumnInfo implements ColumnInfo {
        private final String name, cql3Type;
        private final boolean clustering;
        SimpleColumnInfo(String name, String cql3Type, boolean clustering) {
            this.name = name; this.cql3Type = cql3Type; this.clustering = clustering;
        }
        @Override public String name() { return name; }
        @Override public String cql3Type() { return cql3Type; }
        @Override public boolean isClusteringKey() { return clustering; }
    }
}
