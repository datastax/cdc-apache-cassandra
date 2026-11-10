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
import com.datastax.oss.cdc.Constants;
import com.datastax.oss.cdc.ConverterAndQuery;
import com.datastax.oss.cdc.MutationCache;
import com.datastax.oss.cdc.MutationValue;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.util.concurrent.RateLimiter;
import io.vavr.Tuple2;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SourceContext;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Drives {@link CassandraSource#read()} against a mocked events-topic consumer, covering the batch
 * consume loop, the per-record CQL queries and the records handed to the Pulsar runtime.
 */
class CassandraSourceBatchReadTest {

    private static final Logger SOURCE_LOG = (Logger) LoggerFactory.getLogger(CassandraSource.class);
    private static Level previousLevel;

    private final UUID nodeId = UUID.randomUUID();
    private CassandraSource source;
    private CassandraClient cassandraClient;
    private Consumer<KeyValue<GenericRecord, MutationValue>> consumer;
    private Converter valueConverter;
    private Converter keyConverter;
    private OrderedExecutor executor;

    private static CassandraSourceConnectorConfig config(Map<String, String> extra) {
        Map<String, String> props = new HashMap<>();
        props.put(CassandraSourceConnectorConfig.KEYSPACE_NAME_CONFIG, "ks1");
        props.put(CassandraSourceConnectorConfig.TABLE_NAME_CONFIG, "t1");
        props.put(CassandraSourceConnectorConfig.EVENTS_TOPIC_NAME_CONFIG, "events-ks1.t1");
        props.put(CassandraSourceConnectorConfig.QUERY_BACKOFF_IN_MS_CONFIG, "0");
        props.putAll(extra);
        return new CassandraSourceConnectorConfig(props);
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field f = CassandraSource.class.getDeclaredField(name);
        f.setAccessible(true);
        f.set(target, value);
    }

    @SuppressWarnings("unchecked")
    private void createSource(CassandraSourceConnectorConfig config) throws Exception {
        source = new CassandraSource();
        source.config = config;
        cassandraClient = mock(CassandraClient.class);
        source.cassandraClient = cassandraClient;
        executor = OrderedExecutor.newBuilder().name("batch-read-test").numThreads(2).maxTasksInQueue(1000).build();
        setField(source, "queryExecutor", executor);
        source.mutationCache = new MutationCache<>(3, 100, Duration.ofMinutes(1));
        setField(source, "buffer", new ArrayBlockingQueue<>(config.getBatchSize()));

        ConverterAndQuery<Converter> converterAndQuery = mock(ConverterAndQuery.class);
        valueConverter = mock(Converter.class);
        when(converterAndQuery.getConverter()).thenReturn(valueConverter);
        when(converterAndQuery.prepareSelectStatement(any(), anyInt())).thenReturn(mock(PreparedStatement.class));
        source.valueConverterAndQuery = converterAndQuery;

        keyConverter = mock(Converter.class);
        when(keyConverter.fromConnectData(any())).thenReturn("pk-json");
        source.keyConverter = keyConverter;
        Converter mutationKeyConverter = mock(Converter.class);
        when(mutationKeyConverter.fromConnectData(any())).thenReturn(Collections.singletonList(1));
        source.mutationKeyConverter = mutationKeyConverter;

        consumer = mock(Consumer.class);
        source.consumer = consumer;
        source.sourceContext = mock(SourceContext.class);
    }

    @SuppressWarnings("unchecked")
    private Message<KeyValue<GenericRecord, MutationValue>> message(String key, String md5) {
        Message<KeyValue<GenericRecord, MutationValue>> msg = mock(Message.class);
        GenericRecord mutationKey = mock(GenericRecord.class);
        when(msg.getValue()).thenReturn(new KeyValue<>(mutationKey, new MutationValue(md5, nodeId, null)));
        when(msg.getKey()).thenReturn(key);
        when(msg.getKeyBytes()).thenReturn(key.getBytes(StandardCharsets.UTF_8));
        when(msg.getMessageId()).thenReturn(mock(MessageId.class));
        when(msg.hasProperty(Constants.WRITETIME)).thenReturn(true);
        when(msg.getProperty(Constants.WRITETIME)).thenReturn(Long.toString(System.currentTimeMillis() * 1000L));
        return msg;
    }

    /** Runs every read at DEBUG so the debug-only log statements are exercised too. */
    @BeforeAll
    static void enableDebugLogging() {
        previousLevel = SOURCE_LOG.getLevel();
        SOURCE_LOG.setLevel(Level.DEBUG);
    }

    @AfterAll
    static void restoreLogging() {
        SOURCE_LOG.setLevel(previousLevel);
    }

    @AfterEach
    void tearDown() {
        if (source != null) {
            source.close();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void readsBatchSkipsCacheHitsAndAcksPublishedRecords() throws Exception {
        createSource(config(Collections.emptyMap()));
        Message<KeyValue<GenericRecord, MutationValue>> fresh1 = message("k1", "md5-1");
        Message<KeyValue<GenericRecord, MutationValue>> duplicate = message("k2", "md5-2");
        Message<KeyValue<GenericRecord, MutationValue>> fresh2 = message("k3", "md5-3");
        source.mutationCache.addMutationMd5("k2", "md5-2");
        when(consumer.receive(1, TimeUnit.SECONDS)).thenReturn(fresh1, duplicate, fresh2, null);
        when(cassandraClient.selectRow(anyList(), any(), any(ConsistencyLevel.class), any(), any()))
                .thenReturn(new Tuple2<>(mock(Row.class), nodeId));
        when(valueConverter.toConnectData(any())).thenReturn("row");

        Record<GenericRecord> first = source.read();
        Record<GenericRecord> second = source.read();

        // The duplicate was dropped and acked straight away; the two fresh rows are published in order.
        verify(consumer).acknowledge(duplicate);
        KeyValue<Object, Object> firstValue = (KeyValue<Object, Object>) (Object) first.getValue();
        // Avro output (the default) publishes the events-topic key bytes as the record key.
        assertArrayEquals("k1".getBytes(StandardCharsets.UTF_8), (byte[]) firstValue.getKey());
        assertEquals("row", firstValue.getValue());
        assertNotNull(second.getValue());
        assertTrue(first.getProperties().containsKey(Constants.WRITETIME));

        first.ack();
        second.ack();
        verify(consumer).acknowledge(fresh1);
        verify(consumer).acknowledge(fresh2);
        assertTrue(source.mutationCache.isMutationProcessed("k1", "md5-1"), "coordinator matched, digest cached");
    }

    @Test
    void retriesAfterReceiveFailure() throws Exception {
        createSource(config(Collections.emptyMap()));
        Message<KeyValue<GenericRecord, MutationValue>> msg = message("k1", "md5-1");
        when(consumer.receive(1, TimeUnit.SECONDS))
                .thenThrow(new PulsarClientException("broker unavailable"))
                .thenReturn(null, msg, null);
        when(cassandraClient.selectRow(anyList(), any(), any(ConsistencyLevel.class), any(), any()))
                .thenReturn(new Tuple2<>(mock(Row.class), nodeId));

        assertNotNull(source.read());
    }

    @Test
    void flushesPartialBatchWhenMaxWaitElapses() throws Exception {
        Map<String, String> extra = new HashMap<>();
        extra.put(CassandraSourceConnectorConfig.BATCH_MAX_WAIT_MS_CONFIG, "1");
        createSource(config(extra));
        // The consumer always has another message, so only the deadline can end the batch early.
        List<Message<KeyValue<GenericRecord, MutationValue>>> messages = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            messages.add(message("k" + i, "md5-" + i));
        }
        when(consumer.receive(1, TimeUnit.SECONDS)).thenAnswer(new org.mockito.stubbing.Answer<Object>() {
            int next = 0;
            @Override
            public Object answer(org.mockito.invocation.InvocationOnMock invocation) {
                return messages.get(next++);
            }
        });
        when(cassandraClient.selectRow(anyList(), any(), any(ConsistencyLevel.class), any(), any()))
                .thenReturn(new Tuple2<>(mock(Row.class), nodeId));

        assertNotNull(source.read());
    }

    @Test
    void doesNotCacheWhenAnotherCoordinatorServedTheRead() throws Exception {
        createSource(config(Collections.emptyMap()));
        setField(source, "queryRateLimiter", RateLimiter.create(1000));
        Message<KeyValue<GenericRecord, MutationValue>> msg = message("k1", "md5-1");
        when(consumer.receive(1, TimeUnit.SECONDS)).thenReturn(msg, (Message) null);
        when(cassandraClient.selectRow(anyList(), any(), any(ConsistencyLevel.class), any(), any()))
                .thenReturn(new Tuple2<>(null, UUID.randomUUID()));

        Record<GenericRecord> rec = source.read();

        assertNull(((KeyValue<?, ?>) (Object) rec.getValue()).getValue(), "deleted row gives a null value");
        assertTrue(!source.mutationCache.isMutationProcessed("k1", "md5-1"));
    }

    @Test
    void jsonOnlyOutputPublishesValueBytes() throws Exception {
        Map<String, String> extra = new HashMap<>();
        extra.put(CassandraSourceConnectorConfig.OUTPUT_FORMAT, "json");
        createSource(config(extra));
        Message<KeyValue<GenericRecord, MutationValue>> msg = message("k1", "md5-1");
        when(consumer.receive(1, TimeUnit.SECONDS)).thenReturn(msg, (Message) null);
        when(cassandraClient.selectRow(anyList(), any(), any(ConsistencyLevel.class), any(), any()))
                .thenReturn(new Tuple2<>(mock(Row.class), nodeId));
        byte[] json = "{\"a\":1}".getBytes(StandardCharsets.UTF_8);
        when(valueConverter.toConnectData(any())).thenReturn(json);

        Record<?> rec = source.read();

        assertArrayEquals(json, (byte[]) rec.getValue());
        CassandraSource.CassandraRecord jsonRecord = (CassandraSource.CassandraRecord) rec;
        assertThrows(UnsupportedOperationException.class, jsonRecord::getKeySchema);
        assertSame(msg, jsonRecord.getMutationMessage());
        assertSame(msg.getValue().getKey(), jsonRecord.getMutationKey());
        assertEquals("md5-1", jsonRecord.getMutationValue().getMd5Digest());
        assertEquals(Collections.singletonList(1), jsonRecord.getPk());
        assertSame(source.valueConverterAndQuery, jsonRecord.getConverterAndQuery());
        CompletableFuture<KeyValue<Object, Object>> replacement =
                CompletableFuture.completedFuture(new KeyValue<>("k".getBytes(StandardCharsets.UTF_8), json));
        jsonRecord.replaceQueryResult(replacement);
        assertSame(replacement, jsonRecord.getQueryResult());
        rec.ack();
        verify(consumer).acknowledge(msg);
    }

    @Test
    void exposesTableIdentityForSchemaListener() throws Exception {
        createSource(config(Collections.emptyMap()));
        assertEquals("ks1", source.getKeyspaceName());
        assertEquals("t1", source.getTableName());
        assertSame(cassandraClient, source.getCassandraClient());
        verify(cassandraClient, never()).close();
        source.close();
        verify(cassandraClient, atLeastOnce()).close();
        source = null;
    }

    @Test
    void recordAccessorsReturnTheConsumedMessageParts() throws Exception {
        createSource(config(Collections.emptyMap()));
        Message<KeyValue<GenericRecord, MutationValue>> msg = message("k1", "md5-1");
        when(consumer.receive(1, TimeUnit.SECONDS)).thenReturn(msg, (Message) null);
        when(cassandraClient.selectRow(anyList(), any(), any(ConsistencyLevel.class), any(), eq("md5-1")))
                .thenReturn(new Tuple2<>(mock(Row.class), nodeId));

        CassandraSource.CassandraRecord rec = (CassandraSource.CassandraRecord) source.read();

        assertSame(msg, rec.getMutationMessage());
        assertSame(msg.getValue().getKey(), rec.getMutationKey());
        assertEquals("md5-1", rec.getMutationValue().getMd5Digest());
        assertEquals(Collections.singletonList(1), rec.getPk());
        assertSame(source.valueConverterAndQuery, rec.getConverterAndQuery());
        assertNotNull(rec.getQueryResult());
    }
}
