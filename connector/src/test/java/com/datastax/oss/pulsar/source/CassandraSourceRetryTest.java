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
package com.datastax.oss.pulsar.source;

import com.datastax.oss.cdc.CassandraClient;
import com.datastax.oss.cdc.CassandraSourceConnectorConfig;
import com.datastax.oss.cdc.ConverterAndQuery;
import com.datastax.oss.cdc.MutationCache;
import com.datastax.oss.cdc.MutationValue;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import io.vavr.Tuple3;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SourceContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the CQL query retry behaviour in {@link CassandraSource}.
 *
 * <p>The retry path under test is:
 * <ol>
 *   <li>{@code submitCqlQuery} submits CQL work to the {@link OrderedExecutor} and returns a
 *       future.</li>
 *   <li>{@code waitForCqlWithRetry} blocks on that future; on {@link java.util.concurrent.CompletionException}
 *       it backs off and re-submits <em>only the CQL query</em> — never re-consumes from the
 *       events topic.</li>
 * </ol>
 *
 * <p>Both private methods are called via reflection. The private {@code CassandraRecord} interface
 * is implemented by a {@link Proxy} so it can be passed directly to
 * {@code waitForCqlWithRetry} without constructing the inner {@code MyKVRecord}.
 */
public class CassandraSourceRetryTest {

    private CassandraSource source;
    private CassandraClient mockClient;
    @SuppressWarnings("unchecked")
    private ConverterAndQuery<Converter> mockCaq;
    @SuppressWarnings("unchecked")
    private Consumer<KeyValue<org.apache.pulsar.client.api.schema.GenericRecord, MutationValue>> mockConsumer;
    private SourceContext mockSourceContext;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() throws Exception {
        source = new CassandraSource();

        CassandraSourceConnectorConfig config = minimalConfig();
        setField(source, "config", config);

        mockClient = mock(CassandraClient.class);
        setField(source, "cassandraClient", mockClient);

        OrderedExecutor executor = OrderedExecutor.newBuilder()
                .name("test-pulsar-retry-executor")
                .numThreads(2)
                .maxTasksInQueue(100)
                .build();
        setField(source, "queryExecutor", executor);

        MutationCache<String> cache = new MutationCache<>(3, 100, Duration.ofMinutes(1));
        setField(source, "mutationCache", cache);

        setField(source, "queryRateLimiter", null);

        mockCaq = mock(ConverterAndQuery.class);
        Converter mockConverter = mock(Converter.class);
        when(mockCaq.getConverter()).thenReturn(mockConverter);
        when(mockCaq.prepareSelectStatement(any(), anyInt())).thenReturn(mock(PreparedStatement.class));
        setField(source, "valueConverterAndQuery", mockCaq);

        setField(source, "emptyValue", null);

        // keyConverter: fromConnectData returns a byte[]
        Converter mockKeyConverter = mock(Converter.class);
        when(mockKeyConverter.fromConnectData(any())).thenReturn(new byte[]{1, 2, 3});
        setField(source, "keyConverter", mockKeyConverter);

        setField(source, "consecutiveUnavailableException", 0L);

        // Mock the Pulsar consumer (only needed for acknowledge() on cache hits).
        mockConsumer = mock(Consumer.class);
        doNothing().when(mockConsumer).acknowledge(any(Message.class));
        setField(source, "consumer", mockConsumer);

        // Mock SourceContext so recordMetric calls don't NPE.
        mockSourceContext = mock(SourceContext.class);
        doNothing().when(mockSourceContext).recordMetric(anyString(), anyDouble());
        when(mockSourceContext.getSourceName()).thenReturn("test-source");
        setField(source, "sourceContext", mockSourceContext);
    }

    @AfterEach
    void tearDown() throws Exception {
        OrderedExecutor executor = (OrderedExecutor) getField(source, "queryExecutor");
        if (executor != null) {
            executor.shutdown();
        }
    }

    /**
     * When {@code cassandraClient.selectRow} throws on the first call and succeeds on the second,
     * {@code waitForCqlWithRetry} must retry exactly once and return the non-null result.
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

        @SuppressWarnings("unchecked")
        Converter valueConverter = mockCaq.getConverter();
        when(valueConverter.toConnectData(mockRow)).thenReturn(new byte[]{10, 20});

        RecordProxy proxy = buildRecord(nodeId, "digest-1");
        proxy.setQueryResult(invokeSubmitCqlQuery(proxy));

        KeyValue<Object, Object> result = invokeWaitForCqlWithRetry(proxy);

        assertNotNull(result, "Expected a KeyValue result after one retry");
        assertNotNull(result.getValue(), "Value should be the converted row bytes");
        verify(mockClient, times(2)).selectRow(anyList(), any(), anyList(), any(), anyString());
    }

    /**
     * When {@code cassandraClient.selectRow} fails three times before succeeding, the retry loop
     * must call selectRow exactly four times and ultimately deliver the result.
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
        Converter valueConverter = mockCaq.getConverter();
        when(valueConverter.toConnectData(mockRow)).thenReturn(new byte[]{5});

        RecordProxy proxy = buildRecord(nodeId, "digest-multi");
        proxy.setQueryResult(invokeSubmitCqlQuery(proxy));

        KeyValue<Object, Object> result = invokeWaitForCqlWithRetry(proxy);

        assertNotNull(result, "Expected a KeyValue result after multiple retries");
        verify(mockClient, times(4)).selectRow(anyList(), any(), anyList(), any(), anyString());
    }

    /**
     * When the mutation digest is already in the dedup cache, {@code submitCqlQuery} must
     * complete the future with {@code null} (cache-hit sentinel) and {@code waitForCqlWithRetry}
     * must return {@code null} without ever calling {@code selectRow}.
     */
    @Test
    void cache_hit_returns_null_without_cql_query() throws Exception {
        String cacheKey = "cache-key-hit";
        String md5 = "digest-cached";

        MutationCache<String> cache = (MutationCache<String>) getField(source, "mutationCache");
        cache.addMutationMd5(cacheKey, md5);

        RecordProxy proxy = buildRecordWithKey(cacheKey, UUID.randomUUID(), md5);
        proxy.setQueryResult(invokeSubmitCqlQuery(proxy));

        KeyValue<Object, Object> result = invokeWaitForCqlWithRetry(proxy);

        assertNull(result, "Cache hit must return null (duplicate-mutation sentinel)");
        verify(mockClient, times(0)).selectRow(anyList(), any(), anyList(), any(), anyString());
        // The consumer must acknowledge the duplicate message so the offset advances.
        verify(mockConsumer, times(1)).acknowledge(any(Message.class));
    }

    /**
     * When {@code selectRow} returns a null row (the row was deleted before read-back), the
     * future completes with a KeyValue whose value is the configured {@code emptyValue} (null
     * here). The result must be non-null (the KeyValue wrapper itself must be present).
     */
    @Test
    void null_row_produces_key_value_with_null_value() throws Exception {
        UUID nodeId = UUID.randomUUID();
        when(mockClient.selectRow(anyList(), any(), anyList(), any(), anyString()))
                .thenReturn(new Tuple3<>(null, ConsistencyLevel.LOCAL_QUORUM, nodeId));

        RecordProxy proxy = buildRecord(nodeId, "digest-delete");
        proxy.setQueryResult(invokeSubmitCqlQuery(proxy));

        KeyValue<Object, Object> result = invokeWaitForCqlWithRetry(proxy);

        assertNotNull(result, "Expected a KeyValue wrapper even for a deleted row");
        assertNull(result.getValue(), "Value should be null (emptyValue) for a deleted row");
        verify(mockClient, times(1)).selectRow(anyList(), any(), anyList(), any(), anyString());
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    /** Minimal config with zero backoff so retry loops finish instantly in tests. */
    private static CassandraSourceConnectorConfig minimalConfig() {
        Map<String, String> props = new HashMap<>();
        props.put(CassandraSourceConnectorConfig.KEYSPACE_NAME_CONFIG, "ks1");
        props.put(CassandraSourceConnectorConfig.TABLE_NAME_CONFIG, "t1");
        props.put(CassandraSourceConnectorConfig.EVENTS_TOPIC_NAME_CONFIG, "events-ks1.t1");
        props.put(CassandraSourceConnectorConfig.QUERY_BACKOFF_IN_MS_CONFIG, "0");
        props.put(CassandraSourceConnectorConfig.QUERY_MAX_BACKOFF_IN_SEC_CONFIG, "3600");
        return new CassandraSourceConnectorConfig(props);
    }

    private RecordProxy buildRecord(UUID nodeId, String md5) {
        return buildRecordWithKey("msg-key-" + md5, nodeId, md5);
    }

    @SuppressWarnings("unchecked")
    private RecordProxy buildRecordWithKey(String msgKey, UUID nodeId, String md5) {
        MutationValue mutationValue = new MutationValue(md5, nodeId, null);

        // Mock a Pulsar Message with the minimal surface the production code calls.
        Message<KeyValue<org.apache.pulsar.client.api.schema.GenericRecord, MutationValue>> msg =
                mock(Message.class);
        when(msg.getKey()).thenReturn(msgKey);
        when(msg.getKeyBytes()).thenReturn(msgKey.getBytes());
        when(msg.getMessageId()).thenReturn(mock(MessageId.class));
        when(msg.hasProperty(anyString())).thenReturn(false);

        // Mock mutationKey (Pulsar GenericRecord).
        org.apache.pulsar.client.api.schema.GenericRecord mutationKey =
                mock(org.apache.pulsar.client.api.schema.GenericRecord.class);
        when(mutationKey.getNativeObject()).thenReturn(new Object());

        List<Object> pk = Collections.singletonList("row1");
        return new RecordProxy(msg, mutationKey, mutationValue, pk);
    }

    /**
     * Invokes the private {@code submitCqlQuery} method via reflection, passing a mocked
     * {@code Message} plus the pre-decoded fields that the production code normally computes
     * during {@code batchRead}.
     */
    @SuppressWarnings("unchecked")
    private CompletableFuture<KeyValue<Object, Object>> invokeSubmitCqlQuery(RecordProxy proxy)
            throws Exception {
        Method m = CassandraSource.class.getDeclaredMethod(
                "submitCqlQuery",
                Message.class,
                org.apache.pulsar.client.api.schema.GenericRecord.class,
                MutationValue.class,
                List.class,
                ConverterAndQuery.class);
        m.setAccessible(true);
        return (CompletableFuture<KeyValue<Object, Object>>) m.invoke(
                source,
                proxy.msg,
                proxy.mutationKey,
                proxy.mutationValue,
                proxy.pk,
                mockCaq);
    }

    /**
     * Invokes the private {@code waitForCqlWithRetry} method via reflection, wrapping the proxy's
     * mutable state in a dynamic proxy that implements the private {@code CassandraRecord}
     * interface.
     */
    @SuppressWarnings("unchecked")
    private KeyValue<Object, Object> invokeWaitForCqlWithRetry(RecordProxy proxy) throws Exception {
        // Locate the private CassandraRecord interface by name.
        Class<?> cassandraRecordIface = null;
        for (Class<?> c : CassandraSource.class.getDeclaredClasses()) {
            if (c.getSimpleName().equals("CassandraRecord")) {
                cassandraRecordIface = c;
                break;
            }
        }
        if (cassandraRecordIface == null) {
            throw new IllegalStateException("CassandraRecord interface not found in CassandraSource");
        }

        // Build a dynamic proxy that routes every CassandraRecord call to our RecordProxy.
        final Class<?> ifaceClass = cassandraRecordIface;
        InvocationHandler handler = (proxyObj, method, args) -> {
            switch (method.getName()) {
                case "getMutationMessage":    return proxy.msg;
                case "getQueryResult":        return proxy.getQueryResult();
                case "getConverterAndQuery":  return mockCaq;
                case "getMutationKey":        return proxy.mutationKey;
                case "getMutationValue":      return proxy.mutationValue;
                case "getPk":                 return proxy.pk;
                case "replaceQueryResult":
                    proxy.setQueryResult((CompletableFuture<KeyValue<Object, Object>>) args[0]);
                    return null;
                // KVRecord / Record methods not needed by waitForCqlWithRetry — return defaults.
                default:
                    if (method.getReturnType() == boolean.class) return false;
                    if (method.getReturnType() == int.class)     return 0;
                    return null;
            }
        };
        Object cassandraRecord = Proxy.newProxyInstance(
                CassandraSource.class.getClassLoader(),
                new Class[]{ifaceClass},
                handler);

        Method m = CassandraSource.class.getDeclaredMethod("waitForCqlWithRetry", ifaceClass);
        m.setAccessible(true);
        return (KeyValue<Object, Object>) m.invoke(source, cassandraRecord);
    }

    // ── reflection utilities ──────────────────────────────────────────────────────

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
     * Holds the pre-decoded state for one test record and tracks the mutable
     * {@code queryResult} future so the dynamic proxy can forward
     * {@code getQueryResult}/{@code replaceQueryResult} correctly.
     */
    private static class RecordProxy {
        final Message<KeyValue<org.apache.pulsar.client.api.schema.GenericRecord, MutationValue>> msg;
        final org.apache.pulsar.client.api.schema.GenericRecord mutationKey;
        final MutationValue mutationValue;
        final List<Object> pk;
        private final AtomicReference<CompletableFuture<KeyValue<Object, Object>>> queryResult =
                new AtomicReference<>();

        RecordProxy(
                Message<KeyValue<org.apache.pulsar.client.api.schema.GenericRecord, MutationValue>> msg,
                org.apache.pulsar.client.api.schema.GenericRecord mutationKey,
                MutationValue mutationValue,
                List<Object> pk) {
            this.msg = msg;
            this.mutationKey = mutationKey;
            this.mutationValue = mutationValue;
            this.pk = pk;
        }

        void setQueryResult(CompletableFuture<KeyValue<Object, Object>> future) {
            queryResult.set(future);
        }

        CompletableFuture<KeyValue<Object, Object>> getQueryResult() {
            return queryResult.get();
        }
    }
}
