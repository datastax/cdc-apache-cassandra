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
import com.google.common.util.concurrent.RateLimiter;
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

import java.io.File;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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

    @Test
    void sendRateLimiter_nullByDefault() {
        // A fresh sender has no rate limiter at all -- not a very-high-rate one, none.
        // sendMutationAsync() must skip the rate-limiter wait entirely in this case.
        assertNull(sender.sendRateLimiter);
    }

    @Test
    void buildProducerProperties_defaultRateLimitIsZero_leavesRateLimiterNull() {
        AgentConfig cfg = AgentConfig.create(AgentConfig.Platform.KAFKA, "topicPrefix=events-");
        assertEquals(0.0, (double) cfg.get("kafkaMutationSendRateLimit"),
                "sanity check: default must resolve to 0.0, not null");

        sender.buildProducerProperties(cfg);

        assertNull(sender.sendRateLimiter);
    }

    @Test
    void buildProducerProperties_negativeRateLimit_leavesRateLimiterNull() {
        // A negative value isn't a valid Guava RateLimiter rate; it must be treated as
        // unset/unlimited (same as 0), not passed through to RateLimiter.create() and
        // rejected.
        AgentConfig cfg = AgentConfig.create(AgentConfig.Platform.KAFKA,
                "topicPrefix=events-,kafkaMutationSendRateLimit=-5.0");

        assertDoesNotThrow(() -> sender.buildProducerProperties(cfg));

        assertNull(sender.sendRateLimiter);
    }

    @Test
    void buildProducerProperties_infiniteRateLimit_leavesRateLimiterNull() {
        // Double.parseDouble("Infinity") is valid input an operator could actually supply
        // (e.g. a typo'd config value), and Infinity > 0 is true -- without an explicit
        // Double.isFinite() guard this would be passed straight into RateLimiter.create(),
        // which is itself fine, but would leave the limiter unable to ever throttle again
        // after a later setRate() to a finite value (a real Guava 33.5.0-jre bug, verified
        // empirically) -- so it must be rejected the same as any other non-positive value.
        AgentConfig cfg = AgentConfig.create(AgentConfig.Platform.KAFKA,
                "topicPrefix=events-,kafkaMutationSendRateLimit=Infinity");

        assertDoesNotThrow(() -> sender.buildProducerProperties(cfg));

        assertNull(sender.sendRateLimiter);
    }

    @Test
    void buildProducerProperties_positiveRateLimit_createsRateLimiterAtThatRate() {
        AgentConfig cfg = AgentConfig.create(AgentConfig.Platform.KAFKA,
                "topicPrefix=events-,kafkaMutationSendRateLimit=5.0");

        sender.buildProducerProperties(cfg);

        assertNotNull(sender.sendRateLimiter);
        assertEquals(5.0, sender.sendRateLimiter.getRate());
    }

    @Test
    void buildProducerProperties_positiveRateLimit_actuallyThrottlesSends() throws Exception {
        // End-to-end regression test for the real config -> buildProducerProperties path
        // (not just a direct field assignment like the test below): proves a fresh
        // sender, starting with no rate limiter at all, is genuinely reconfigured to
        // throttle through the real AgentConfig resolution. 2 permits/sec over 3 sends:
        // same ~1.0s math as send_withRateLimiterSet_throttlesToConfiguredRate below.
        AgentConfig cfg = AgentConfig.create(AgentConfig.Platform.KAFKA,
                "topicPrefix=events-,kafkaMutationSendRateLimit=2.0");
        sender.buildProducerProperties(cfg);

        long start = System.nanoTime();
        sender.sendMutationAsync(mutation("ks", "tbl", "pk1", 1L, 0)).get();
        sender.sendMutationAsync(mutation("ks", "tbl", "pk2", 2L, 0)).get();
        sender.sendMutationAsync(mutation("ks", "tbl", "pk3", 3L, 0)).get();
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertTrue(elapsedMs >= 800,
                "expected throttled sends to take >= 800ms, took " + elapsedMs + "ms");
    }

    @Test
    void send_withRateLimiterSet_throttlesToConfiguredRate() throws Exception {
        // Guava's RateLimiter.create()/setRate() starts a fresh "bucket" that lets the
        // first acquire() through immediately (no stored permits consumed yet). At
        // 2 permits/sec, the 2nd and 3rd acquire() each then have to wait ~1/2 = 0.5s
        // for their permit, so 3 sequential sends cost ~0 + 0.5 + 0.5 = ~1.0s total.
        // Assert >= 800ms (not >= 1000ms) to leave headroom for scheduling/GC jitter
        // while still clearly proving real throttling happened: the equivalent
        // unthrottled run (sendRateLimiter null by default, as used by every other
        // test in this class) completes in low single-digit ms.
        sender.sendRateLimiter = RateLimiter.create(2.0);

        long start = System.nanoTime();
        sender.sendMutationAsync(mutation("ks", "tbl", "pk1", 1L, 0)).get();
        sender.sendMutationAsync(mutation("ks", "tbl", "pk2", 2L, 0)).get();
        sender.sendMutationAsync(mutation("ks", "tbl", "pk3", 3L, 0)).get();
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertTrue(elapsedMs >= 800,
                "expected throttled sends to take >= 800ms, took " + elapsedMs + "ms");
        assertEquals(3, mockProducer.history().size());
        // Each ~0.5s wait is polled in ACQUIRE_POLL_TIMEOUT (100ms) steps, so each of the
        // 2 throttled sends should have needed several retries -- verifies the public
        // getSendRateLimiterRetries() getter (review comment: "add metrics for these")
        // is actually wired up, not just present and unused.
        assertTrue(sender.getSendRateLimiterRetries() > 0,
                "expected at least one tracked retry on the rate limiter, got " + sender.getSendRateLimiterRetries());
        // Nothing else was sending concurrently in this single-threaded test, and
        // pendingSemaphore starts with 1000 permits, so it should never have been
        // contended -- confirms getPendingSemaphoreRetries() stays accurate (0) rather
        // than, say, double-counting or firing on every acquire regardless of contention.
        assertEquals(0, sender.getPendingSemaphoreRetries(),
                "pending semaphore was not contended in this single-threaded test");
    }

    @Test
    void send_withRateLimiterSet_andProducerError_futureFailsAndPermitReleased() {
        // Combines a configured rate limit with an async producer failure (comment:
        // "also test with failing producer"): the rate limiter must not interfere with
        // the existing failure/permit-release path. Rate set high so the test stays fast.
        sender.sendRateLimiter = RateLimiter.create(10_000.0);
        mockProducer = new MockProducer<>(false, new ByteArraySerializer(), new ByteArraySerializer());
        sender.producer = mockProducer;

        CompletableFuture<?> f = sender.sendMutationAsync(mutation("ks", "tbl", "pk1", 1L, 0));
        mockProducer.errorNext(new RuntimeException("broker unavailable"));

        assertThrows(Exception.class, f::get);
        assertEquals(1000, sender.pendingSemaphore.availablePermits(),
                "permit must be released on async failure, not leaked");
    }

    @Test
    void send_producerThrowsSynchronously_semaphoreReleasedAndFutureFails() {
        // producer.send() can throw synchronously (e.g. a closed/misbehaving producer)
        // before ever registering the success/failure callback. Without releasing the
        // semaphore in that path, the permit acquired just before send() would leak
        // forever. MockProducer.send() after close() is a reliable, real way to trigger
        // exactly this: it throws IllegalStateException synchronously, not via callback.
        mockProducer.close();

        CompletableFuture<?> f = sender.sendMutationAsync(mutation("ks", "tbl", "pk1", 1L, 0));

        assertTrue(f.isCompletedExceptionally());
        assertEquals(1000, sender.pendingSemaphore.availablePermits(),
                "permit must be released when send() throws synchronously, not leaked");
    }

    @Test
    void send_threadInterruptedWhileWaiting_restoresInterruptFlag() throws Exception {
        // Thread.sleep() / pendingSemaphore.tryAcquire(timeout, unit) inside
        // sendMutationAsync() both throw InterruptedException. The outer catch must
        // restore the interrupt flag (Thread.currentThread().interrupt()) rather than
        // swallowing it, or callers relying on it (e.g. a thread pool's shutdown check)
        // would never see that this thread was interrupted.
        sender.sendRateLimiter = RateLimiter.create(0.001); // ~1000s/permit: next acquire is guaranteed to wait
        sender.sendRateLimiter.acquire();                   // consume the one free initial permit

        AtomicBoolean interruptedAfter = new AtomicBoolean();
        AtomicReference<CompletableFuture<?>> futureRef = new AtomicReference<>();
        Thread t = new Thread(() -> {
            futureRef.set(sender.sendMutationAsync(mutation("ks", "tbl", "pk1", 1L, 0)));
            interruptedAfter.set(Thread.currentThread().isInterrupted());
        });
        t.start();
        Thread.sleep(50); // let it enter the sleep/tryAcquire wait
        t.interrupt();
        t.join(2000);

        assertFalse(t.isAlive(), "sender thread should have returned, not hung, after interrupt");
        assertTrue(interruptedAfter.get(), "interrupt flag must be restored on the thread, not swallowed");
        assertNotNull(futureRef.get());
        assertTrue(futureRef.get().isCompletedExceptionally());
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
    // initialize — config file pass-through
    // -------------------------------------------------------------------------

    @Test
    void initialize_passthroughForwardsDottedAndNonDottedKeys() throws Exception {
        // Write a temp config file with both a dotted key (compression.type)
        // and a non-dotted key (acks) — the old code silently dropped non-dotted ones.
        File tmpFile = File.createTempFile("kafka-test-", ".properties");
        tmpFile.deleteOnExit();
        try (FileWriter w = new FileWriter(tmpFile)) {
            w.write("bootstrapServers=localhost:9092\n");
            w.write("acks=all\n");
            w.write("compression.type=lz4\n");
        }

        AgentConfig cfg = AgentConfig.create(AgentConfig.Platform.KAFKA,
                "topicPrefix=events-,kafkaConfigFile=" + tmpFile.getAbsolutePath());

        Properties props = sender.buildProducerProperties(cfg);

        assertEquals("all",  props.getProperty("acks"),             "acks (no dot) must be forwarded");
        assertEquals("lz4",  props.getProperty("compression.type"), "compression.type (dotted) must be forwarded");
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
