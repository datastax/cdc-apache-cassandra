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
package com.datastax.oss.kafka.source;

import com.datastax.oss.cdc.CassandraClient;
import com.datastax.oss.cdc.CassandraSourceConnectorConfig;
import com.datastax.oss.cdc.ConverterAndQuery;
import com.datastax.oss.cdc.MutationCache;
import com.datastax.oss.cdc.MutationValue;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.kafka.source.converters.Converter;
import io.vavr.Tuple3;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the CQL query retry behaviour in {@link KafkaCassandraSourceTask}.
 *
 * <p>The production path is:
 * <ol>
 *   <li>{@code submitCqlQuery} submits the CQL work to the {@link OrderedExecutor} and returns a
 *       future.</li>
 *   <li>{@code waitForCqlWithRetry} blocks on that future; when it completes exceptionally it
 *       backs off and re-submits <em>only the CQL query</em> (never re-polls the events topic).</li>
 * </ol>
 *
 * <p>Tests here exercise these two private methods via reflection so that the full retry loop
 * can be verified without standing up a real Kafka broker or a real Cassandra node.
 */
public class KafkaCassandraSourceTaskRetryTest {

    private KafkaCassandraSourceTask task;
    private CassandraClient mockClient;
    private ConverterAndQuery<Converter<byte[], ?>> mockCaq;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() throws Exception {
        task = new KafkaCassandraSourceTask();

        // Minimal config: 0 ms backoff so tests run fast.
        CassandraSourceConnectorConfig config = minimalConfig();
        setField(task, "config", config);

        mockClient = mock(CassandraClient.class);
        setField(task, "cassandraClient", mockClient);

        // A real OrderedExecutor so tasks actually execute.
        OrderedExecutor executor = OrderedExecutor.newBuilder()
                .name("test-retry-executor")
                .numThreads(2)
                .maxTasksInQueue(100)
                .build();
        setField(task, "queryExecutor", executor);

        // Empty mutation cache.
        MutationCache<String> cache = new MutationCache<>(3, 100, Duration.ofMinutes(1));
        setField(task, "mutationCache", cache);

        // Rate limiter disabled.
        setField(task, "queryRateLimiter", null);

        // valueConverterAndQuery mock.
        mockCaq = mock(ConverterAndQuery.class);
        when(mockCaq.getConverter()).thenReturn(mock(Converter.class));
        when(mockCaq.prepareSelectStatement(any(), anyInt())).thenReturn(mock(PreparedStatement.class));
        setField(task, "valueConverterAndQuery", mockCaq);

        // emptyValue
        setField(task, "emptyValue", null);

        // Mock key converter.
        Converter<byte[], ?> keyConverter = mock(Converter.class);
        when(keyConverter.fromConnectData(any(GenericRecord.class))).thenReturn(new byte[]{1, 2, 3});
        setField(task, "keyConverter", keyConverter);

        // consecutiveUnavailableException counter
        setField(task, "consecutiveUnavailableException", 0L);

        // output + heartbeat topics
        setField(task, "outputTopic", "output-topic");
        setField(task, "heartbeatTopic", "output-topic-heartbeat");
    }

    @AfterEach
    void tearDown() throws Exception {
        OrderedExecutor executor = (OrderedExecutor) getField(task, "queryExecutor");
        if (executor != null) {
            executor.shutdown();
        }
    }

    /**
     * When {@code cassandraClient.selectRow} throws on the first call and succeeds on the second,
     * {@code waitForCqlWithRetry} must retry exactly once and return the result from the second call.
     */
    @Test
    void retries_once_after_single_cql_failure() throws Exception {
        Row mockRow = mock(Row.class);
        UUID nodeId = UUID.randomUUID();

        AtomicInteger callCount = new AtomicInteger(0);
        when(mockClient.selectRow(anyList(), any(), anyList(), any(), anyString()))
                .thenAnswer(inv -> {
                    if (callCount.incrementAndGet() == 1) {
                        throw new RuntimeException("Simulated CQL failure");
                    }
                    return new Tuple3<>(mockRow, ConsistencyLevel.LOCAL_QUORUM, nodeId);
                });

        // Converter returns non-null bytes for the row.
        @SuppressWarnings("unchecked")
        Converter<byte[], ?> valueConverter = (Converter<byte[], ?>) mockCaq.getConverter();
        when(valueConverter.toConnectData(mockRow)).thenReturn(new byte[]{10, 20});

        DecodedRecordProxy decoded = buildDecodedRecord(nodeId, "digest-1");

        // Submit the initial CQL query.
        CompletableFuture<SourceRecord> future = invokeSubmitCqlQuery(decoded, mockCaq);
        decoded.setQueryResult(future);

        // waitForCqlWithRetry should recover and return a non-null record.
        SourceRecord result = invokeWaitForCqlWithRetry(decoded);

        assertNotNull(result, "Expected a SourceRecord after one retry");
        assertEquals("output-topic", result.topic());
        verify(mockClient, times(2)).selectRow(anyList(), any(), anyList(), any(), anyString());
    }

    /**
     * When {@code cassandraClient.selectRow} fails three times before succeeding, the retry loop
     * must try exactly four times in total and ultimately deliver the result.
     */
    @Test
    void retries_multiple_times_until_success() throws Exception {
        Row mockRow = mock(Row.class);
        UUID nodeId = UUID.randomUUID();

        AtomicInteger callCount = new AtomicInteger(0);
        when(mockClient.selectRow(anyList(), any(), anyList(), any(), anyString()))
                .thenAnswer(inv -> {
                    if (callCount.incrementAndGet() < 4) {
                        throw new RuntimeException("Simulated CQL failure #" + callCount.get());
                    }
                    return new Tuple3<>(mockRow, ConsistencyLevel.LOCAL_QUORUM, nodeId);
                });

        @SuppressWarnings("unchecked")
        Converter<byte[], ?> valueConverter = (Converter<byte[], ?>) mockCaq.getConverter();
        when(valueConverter.toConnectData(mockRow)).thenReturn(new byte[]{5});

        DecodedRecordProxy decoded = buildDecodedRecord(nodeId, "digest-multi");
        decoded.setQueryResult(invokeSubmitCqlQuery(decoded, mockCaq));

        SourceRecord result = invokeWaitForCqlWithRetry(decoded);

        assertNotNull(result, "Expected a SourceRecord after multiple retries");
        verify(mockClient, times(4)).selectRow(anyList(), any(), anyList(), any(), anyString());
    }

    /**
     * When the mutation digest is already cached (dedup cache hit), {@code submitCqlQuery} must
     * emit a heartbeat record and {@code waitForCqlWithRetry} must return it without calling
     * {@code selectRow} at all.
     */
    @Test
    void cache_hit_skips_cql_query_and_returns_heartbeat() throws Exception {
        String cacheKey = "cached-key";
        String md5 = "digest-cached";

        // Pre-populate the cache.
        MutationCache<String> cache = (MutationCache<String>) getField(task, "mutationCache");
        cache.addMutationMd5(cacheKey, md5);

        DecodedRecordProxy decoded = buildDecodedRecordWithCacheKey(cacheKey, UUID.randomUUID(), md5);
        decoded.setQueryResult(invokeSubmitCqlQuery(decoded, mockCaq));

        SourceRecord result = invokeWaitForCqlWithRetry(decoded);

        // Should be a heartbeat (non-null, sent to heartbeat topic).
        assertNotNull(result, "Expected a heartbeat record for cache hit");
        assertEquals("output-topic-heartbeat", result.topic());
        verify(mockClient, times(0)).selectRow(anyList(), any(), anyList(), any(), anyString());
    }

    /**
     * When a null row is returned (the row was deleted before the read-back), the resulting
     * SourceRecord should still be emitted (using {@code emptyValue = null}).
     */
    @Test
    void null_row_produces_source_record_with_null_value() throws Exception {
        UUID nodeId = UUID.randomUUID();
        // selectRow returns null row (row was deleted)
        when(mockClient.selectRow(anyList(), any(), anyList(), any(), anyString()))
                .thenReturn(new Tuple3<>(null, ConsistencyLevel.LOCAL_QUORUM, nodeId));

        DecodedRecordProxy decoded = buildDecodedRecord(nodeId, "digest-delete");
        decoded.setQueryResult(invokeSubmitCqlQuery(decoded, mockCaq));

        SourceRecord result = invokeWaitForCqlWithRetry(decoded);

        assertNotNull(result, "Expected a SourceRecord even for a deleted row");
        assertNull(result.value(), "Value should be null (emptyValue) for a deleted row");
        verify(mockClient, times(1)).selectRow(anyList(), any(), anyList(), any(), anyString());
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    /**
     * Minimal in-process config with zero backoff so retry loops complete instantly.
     */
    private static CassandraSourceConnectorConfig minimalConfig() {
        java.util.Map<String, String> props = new java.util.HashMap<>();
        props.put(CassandraSourceConnectorConfig.KEYSPACE_NAME_CONFIG, "ks1");
        props.put(CassandraSourceConnectorConfig.TABLE_NAME_CONFIG, "t1");
        props.put(CassandraSourceConnectorConfig.EVENTS_TOPIC_NAME_CONFIG, "events-ks1.t1");
        props.put(CassandraSourceConnectorConfig.QUERY_BACKOFF_IN_MS_CONFIG, "0");
        props.put(CassandraSourceConnectorConfig.QUERY_MAX_BACKOFF_IN_SEC_CONFIG, "3600");
        props.put(CassandraSourceConnectorConfig.OUTPUT_TOPIC_CONFIG, "output-topic");
        props.put(CassandraSourceConnectorConfig.INTERNAL_CONSUMER_BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        return new CassandraSourceConnectorConfig(props);
    }

    /** Builds a {@link DecodedRecordProxy} backed by a real {@link ConsumerRecord}. */
    private DecodedRecordProxy buildDecodedRecord(UUID nodeId, String md5) throws Exception {
        return buildDecodedRecordWithCacheKey("test-cache-key-" + md5, nodeId, md5);
    }

    private DecodedRecordProxy buildDecodedRecordWithCacheKey(String cacheKey, UUID nodeId, String md5) throws Exception {
        ConsumerRecord<byte[], byte[]> rec = new ConsumerRecord<>(
                "events-ks1.t1", 0, 0L, new byte[]{1}, new byte[]{2});
        MutationValue mutationValue = new MutationValue(md5, nodeId, null);

        // Build a minimal Avro GenericRecord for the mutationKeyRecord field.
        Schema avroSchema = SchemaBuilder.record("MutationKey").fields()
                .name("id").type().stringType().noDefault()
                .endRecord();
        GenericRecord mutationKeyRecord = new GenericData.Record(avroSchema);
        mutationKeyRecord.put("id", "row1");

        List<Object> pk = Collections.singletonList("row1");
        return new DecodedRecordProxy(rec, mutationKeyRecord, mutationValue, pk, cacheKey);
    }

    /** Invokes the private {@code submitCqlQuery} via reflection. */
    @SuppressWarnings("unchecked")
    private CompletableFuture<SourceRecord> invokeSubmitCqlQuery(
            DecodedRecordProxy decoded, ConverterAndQuery<Converter<byte[], ?>> caq) throws Exception {
        // Build the real inner DecodedRecord instance from the proxy.
        Object innerDecoded = decoded.toInner(task);
        Method m = KafkaCassandraSourceTask.class.getDeclaredMethod(
                "submitCqlQuery", Class.forName(
                        "com.datastax.oss.kafka.source.KafkaCassandraSourceTask$DecodedRecord"),
                ConverterAndQuery.class);
        m.setAccessible(true);
        return (CompletableFuture<SourceRecord>) m.invoke(task, innerDecoded, caq);
    }

    /** Invokes the private {@code waitForCqlWithRetry} via reflection. */
    private SourceRecord invokeWaitForCqlWithRetry(DecodedRecordProxy decoded) throws Exception {
        Object innerDecoded = decoded.toInner(task);
        // Update the queryResult field on the inner object to match the proxy's current future.
        Field qrField = innerDecoded.getClass().getDeclaredField("queryResult");
        qrField.setAccessible(true);
        qrField.set(innerDecoded, decoded.getQueryResult());

        Method m = KafkaCassandraSourceTask.class.getDeclaredMethod(
                "waitForCqlWithRetry", Class.forName(
                        "com.datastax.oss.kafka.source.KafkaCassandraSourceTask$DecodedRecord"));
        m.setAccessible(true);
        return (SourceRecord) m.invoke(task, innerDecoded);
    }

    // ── reflection helpers ────────────────────────────────────────────────────────

    private static void setField(Object target, String name, Object value) throws Exception {
        Class<?> clazz = target.getClass();
        while (clazz != null) {
            try {
                Field f = clazz.getDeclaredField(name);
                f.setAccessible(true);
                f.set(target, value);
                return;
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        throw new NoSuchFieldException(name + " not found in " + target.getClass());
    }

    private static Object getField(Object target, String name) throws Exception {
        Class<?> clazz = target.getClass();
        while (clazz != null) {
            try {
                Field f = clazz.getDeclaredField(name);
                f.setAccessible(true);
                return f.get(target);
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        throw new NoSuchFieldException(name + " not found in " + target.getClass());
    }

    /**
     * A value-holder that mirrors the private inner {@code DecodedRecord} class, with a mutable
     * {@code queryResult}. Used to set up test state; converted to a real inner instance before
     * being passed to the task's private methods.
     */
    private static class DecodedRecordProxy {
        final ConsumerRecord<byte[], byte[]> rec;
        final GenericRecord mutationKeyRecord;
        final MutationValue mutationValue;
        final List<Object> pk;
        final String cacheKey;
        private CompletableFuture<SourceRecord> queryResult;

        // Cache the constructed inner object so we always return the same instance.
        private Object innerInstance;

        DecodedRecordProxy(ConsumerRecord<byte[], byte[]> rec, GenericRecord mutationKeyRecord,
                           MutationValue mutationValue, List<Object> pk, String cacheKey) {
            this.rec = rec;
            this.mutationKeyRecord = mutationKeyRecord;
            this.mutationValue = mutationValue;
            this.pk = pk;
            this.cacheKey = cacheKey;
        }

        void setQueryResult(CompletableFuture<SourceRecord> future) {
            this.queryResult = future;
        }

        CompletableFuture<SourceRecord> getQueryResult() {
            return queryResult;
        }

        /** Lazily constructs (and caches) the real private inner-class instance via reflection. */
        Object toInner(KafkaCassandraSourceTask task) throws Exception {
            if (innerInstance != null) return innerInstance;
            Class<?> innerClass = Class.forName(
                    "com.datastax.oss.kafka.source.KafkaCassandraSourceTask$DecodedRecord");
            java.lang.reflect.Constructor<?> ctor = innerClass.getDeclaredConstructor(
                    ConsumerRecord.class, GenericRecord.class,
                    MutationValue.class, List.class, String.class);
            ctor.setAccessible(true);
            innerInstance = ctor.newInstance(rec, mutationKeyRecord, mutationValue, pk, cacheKey);
            return innerInstance;
        }
    }
}
