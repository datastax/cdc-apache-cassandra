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
import com.datastax.oss.cdc.SourceUtil;
import com.datastax.oss.cdc.CassandraSourceConnectorConfig;
import com.datastax.oss.cdc.ConfigUtil;
import com.datastax.oss.cdc.Constants;
import com.datastax.oss.cdc.ConverterAndQuery;
import com.datastax.oss.cdc.CqlLogicalTypes;
import com.datastax.oss.cdc.MutationCache;
import com.datastax.oss.cdc.MutationValue;
import com.datastax.oss.cdc.SourceSchemaChangeListener;
import com.datastax.oss.cdc.Version;
import com.datastax.oss.cdc.converters.ConverterFactory;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.pulsar.source.converters.PulsarAvroConverter;
import com.datastax.oss.pulsar.source.converters.PulsarJsonConverter;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.vavr.Tuple2;
import io.vavr.Tuple3;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Conversions;
import org.apache.avro.specific.SpecificData;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.KeySharedPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.functions.api.KVRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Source;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

import org.apache.bookkeeper.common.util.OrderedExecutor;

import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Cassandra source that treats incoming cassandra updates on the input events topic
 * and publish rows on the data topic.
 */
@Connector(
        name = "cassandra-source",
        type = IOType.SOURCE,
        help = "The CassandraSource is used for moving data from Cassandra to Pulsar.",
        configClass = CassandraSourceConfig.class)
@Slf4j
public class CassandraSource implements Source<GenericRecord>, SourceSchemaChangeListener {

    /**
     * Metric name for the mutation cache hits.
     */
    public static final String CACHE_HITS = "cache_hits";

    /**
     * Metric name for the number of mutation cache miss.
     */
    public static final String CACHE_MISSES = "cache_misses";

    /**
     * Metric name form the mutation cache eviction count.
     */
    public static final String CACHE_EVICTIONS = "cache_evictions";

    /**
     * Metric name form the mutation cache estimated size.
     */
    public static final String CACHE_SIZE = "cache_size";

    /**
     * Metric name for the CQL query latency in milliseconds.
     */
    public static final String QUERY_LATENCY = "query_latency";

    /**
     * Metric name for the current number of query executor threads
     */
    public static final String QUERY_EXECUTORS = "query_executors";

    /**
     * The metric name for the replication latency (the Cassandra write time minus the publish time)
     */
    public static final String REPLICATION_LATENCY = "replication_latency";

    SourceContext sourceContext;
    CassandraSourceConnectorConfig config;
    Consumer<KeyValue<GenericRecord, MutationValue>> consumer = null;
    CassandraClient cassandraClient;

    String dirtyTopicName;
    Converter mutationKeyConverter;
    Converter keyConverter;

    Optional<Pattern> columnPattern = Optional.empty();

    MutationCache<String> mutationCache;

    final Schema<KeyValue<GenericRecord, MutationValue>> eventsSchema = Schema.KeyValue(
            Schema.AUTO_CONSUME(),
            Schema.AVRO(MutationValue.class),
            KeyValueEncodingType.SEPARATED);

    /**
     * Converter and CQL query parameters updated on CQL schema update.
     */
    volatile ConverterAndQuery<Converter> valueConverterAndQuery;

    /**
     * Holds an empty value for use with delete mutations. The empty value life cycle is coupled with the
     * valueConverterAndQuery life cycle and is meant to avoid re-creating empty values for delete mutations.
     */
    private Object emptyValue;

    /**
     * Fixed-size ordered executor. All tasks for the same key run in submission order,
     * preventing out-of-order CQL read-backs for the same partition key.
     * When the per-thread task queue is full a {@link java.util.concurrent.RejectedExecutionException}
     * is thrown and treated as backpressure by nacking the batch and backing off.
     */
    private OrderedExecutor queryExecutor;
    private long consecutiveUnavailableException = 0;

    private ArrayBlockingQueue<CassandraRecord> buffer;

    public CassandraSource() {
        // register AVRO logical types conversion
        SpecificData.get().addLogicalTypeConversion(new CqlLogicalTypes.CqlVarintConversion());
        SpecificData.get().addLogicalTypeConversion(new CqlLogicalTypes.CqlDecimalConversion());
        SpecificData.get().addLogicalTypeConversion(new PulsarAvroConverter.CqlDurationConversion());
        SpecificData.get().addLogicalTypeConversion(new Conversions.UUIDConversion());
    }

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) {
        try {
            this.sourceContext = sourceContext;
            this.config = new CassandraSourceConnectorConfig(ConfigUtil.flatString(config));
            this.buffer = new ArrayBlockingQueue<>(this.config.getBatchSize());
            if (!Strings.isNullOrEmpty(this.config.getColumnsRegexp()) && !".*".equals(this.config.getColumnsRegexp())) {
                this.columnPattern = Optional.of(Pattern.compile(this.config.getColumnsRegexp()));
            }

            Preconditions.checkArgument(this.config.getEventsTopic() != null, "Events topic not set");
            this.dirtyTopicName = this.config.getEventsTopic();
            ConsumerBuilder<KeyValue<GenericRecord, MutationValue>> consumerBuilder = sourceContext.newConsumerBuilder(eventsSchema)
                    .consumerName("CDC Consumer")
                    .topic(dirtyTopicName)
                    .subscriptionName(this.config.getEventsSubscriptionName())
                    .subscriptionType(SubscriptionType.valueOf(this.config.getEventsSubscriptionType()))
                    .subscriptionMode(SubscriptionMode.Durable)
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);
            if (SubscriptionType.Key_Shared.equals(SubscriptionType.valueOf(this.config.getEventsSubscriptionType()))) {
                consumerBuilder.keySharedPolicy(KeySharedPolicy.autoSplitHashRange());
            }
            this.consumer = consumerBuilder.subscribe();
            this.mutationCache = new MutationCache<>(
                    this.config.getCacheMaxDigests(),
                    this.config.getCacheMaxCapacity(),
                    Duration.ofMillis(this.config.getCacheExpireAfterMs()));
            log.info("Starting source connector topic={} subscription={} query.executors={} maxTasksInQueue={}",
                    dirtyTopicName,
                    this.config.getEventsSubscriptionName(),
                    this.config.getQueryExecutors(),
                    this.config.getQueryMaxTasksInQueue());
            this.queryExecutor = OrderedExecutor.newBuilder()
                    .name("cdc-query-executor")
                    .numThreads(this.config.getQueryExecutors())
                    .maxTasksInQueue(this.config.getQueryMaxTasksInQueue())
                    .build();
            initCassandraClientWithRetry();
        } catch (Throwable err) {
            log.error("Cannot open the connector:", err);
            throw new RuntimeException(err);
        }
    }

    void initCassandraClientWithRetry() {
        long consecutiveFailures = 0;
        long deadlineMs = System.currentTimeMillis() + this.config.getQueryMaxBackoffInSec() * 1000;
        while (true) {
            try {
                initCassandraClient();
                return;
            } catch (Throwable err) {
                if (System.currentTimeMillis() >= deadlineMs) {
                    throw new RuntimeException("Failed to initialize Cassandra client after " +
                            this.config.getQueryMaxBackoffInSec() + "s, giving up", err);
                }
                consecutiveFailures = SourceUtil.backoffRetry(err, consecutiveFailures, this.config);
            }
        }
    }

    void initCassandraClient() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, InstantiationException {
        this.cassandraClient = new CassandraClient(this.config, Version.getVersion(), sourceContext.getSourceName(), this);
        Tuple2<KeyspaceMetadata, TableMetadata> tuple = cassandraClient.getTableMetadata(this.config.getKeyspaceName(), this.config.getTableName());
        Preconditions.checkArgument(tuple._1 != null, String.format(Locale.ROOT, "Keyspace %s does not exist", this.config.getKeyspaceName()));
        Preconditions.checkArgument(tuple._2 != null, String.format(Locale.ROOT, "Table %s.%s does not exist", this.config.getKeyspaceName(), this.config.getTableName()));
        this.keyConverter = createConverter(getKeyConverterClass(), tuple._1, tuple._2, tuple._2.getPrimaryKey());
        this.mutationKeyConverter = new PulsarAvroConverter(tuple._1, tuple._2, tuple._2.getPrimaryKey());
        setValueConverterAndQuery(tuple._1, tuple._2);
    }

    public synchronized void setValueConverterAndQuery(KeyspaceMetadata ksm, TableMetadata tableMetadata) {
        try {
            this.valueConverterAndQuery = ConverterAndQuery.forTable(
                    config, columnPattern, cassandraClient, ksm, tableMetadata, getValueConverterClass(), log);
            this.emptyValue = config.isJsonOnlyOutputFormat() ? "{}".getBytes(StandardCharsets.UTF_8) : null;
            log.debug("valueConverterAndQuery={}", this.valueConverterAndQuery);
        } catch (Exception e) {
            log.error("Unexpected error", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Build the CQL prepared statement for the specified where clause length.
     * NOTE: The prepared statement cannot be built from the schema listener thread to avoid a possible deadlock.
     *
     * @param valueConverterAndQuery the converter and query parameters
     * @param whereClauseLength      the number of columns in the where clause
     * @return preparedStatement
     */
    synchronized PreparedStatement getSelectStatement(ConverterAndQuery<Converter> valueConverterAndQuery, int whereClauseLength) {
        return valueConverterAndQuery.prepareSelectStatement(cassandraClient, whereClauseLength);
    }

    Class<?> getKeyConverterClass() {
        return ConverterFactory.resolveConverterClass(
                config.getKeyConverterClass(), config.isJsonOutputFormat(), PulsarJsonConverter.class, PulsarAvroConverter.class);
    }

    Class<?> getValueConverterClass() {
        return ConverterFactory.resolveConverterClass(
                config.getValueConverterClass(), config.isJsonOutputFormat(), PulsarJsonConverter.class, PulsarAvroConverter.class);
    }

    Converter createConverter(Class<?> converterClass, KeyspaceMetadata ksm, TableMetadata tableMetadata, List<ColumnMetadata> columns)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        return ConverterFactory.create(converterClass, ksm, tableMetadata, columns);
    }

    CassandraRecord createRecord(ConverterAndQuery<Converter> converterAndQueryFinal, CompletableFuture<KeyValue<Object, Object>> keyValue, Message<KeyValue<GenericRecord, MutationValue>> msg) {
        final MyKVRecord kvRecord = new MyKVRecord(converterAndQueryFinal, keyValue, msg);

        return config.isJsonOnlyOutputFormat() ? new JsonValueRecord(kvRecord) : kvRecord;
    }

    @Override
    public void close() {
        log.info("Closing connector");
        if (this.cassandraClient != null) {
            this.cassandraClient.close();
            this.cassandraClient = null;
        }
        if (queryExecutor != null) {
            queryExecutor.shutdown();
        }
    }

    /**
     * Reads the next message from source.
     * If source does not have any new messages, this call should block.
     *
     * @return next message from source.  The return result should never be null
     * @throws Exception
     */
    @Override
    @SuppressWarnings("unchecked")
    public Record<GenericRecord> read() throws Exception {
        Preconditions.checkState(this.sourceContext != null, "sourceContext should not be null");
        CassandraRecord record = buffer.poll();
        if (record != null) {
            consumer.acknowledge(record.getMutationMessage());
            return (Record) record;
        }
        // this methods returns only if the buffer holds at least one record
        maybeBatchRead();
        record = buffer.poll();
        consumer.acknowledge(record.getMutationMessage());
        return record;
    }

    private void maybeBatchRead() throws Exception {
        Preconditions.checkState(buffer.isEmpty(), "Buffer is not empty");
        List<CassandraRecord> newRecords = batchRead();
        while (newRecords.isEmpty()) {
            newRecords = batchRead();
        }
        buffer.addAll(newRecords);
    }

    @SuppressWarnings("unchecked")
    private List<CassandraRecord> batchRead() throws Exception {
        while (true) {
            List<CassandraRecord> newRecords = new ArrayList<>();
            try {
                // we want to fill the buffer
                // this method will block until we receive at least one record
                while (newRecords.size() < this.config.getBatchSize()) {
                    final Message<KeyValue<GenericRecord, MutationValue>> msg = consumer.receive(1, TimeUnit.SECONDS);
                    if (msg == null) {
                        if (!newRecords.isEmpty()) {
                            if (log.isDebugEnabled()) {
                                log.debug("no message received, buffer size {}", newRecords.size());
                            }
                            // no more records within the timeout, but we have at least one record
                            break;
                        } else {
                            if (log.isDebugEnabled()) {
                                log.debug("no message received");
                            }
                            continue;
                        }
                    }
                    final KeyValue<GenericRecord, MutationValue> kv = msg.getValue();
                    final GenericRecord mutationKey = kv.getKey();
                    final MutationValue mutationValue = kv.getValue();

                    if (log.isDebugEnabled()) {
                        log.debug("Message from producer={} msgId={} key={} value={} schema {}\n",
                                msg.getProducerName(), msg.getMessageId(), kv.getKey(), kv.getValue(), msg.getReaderSchema().orElse(null));
                    }

                    List<Object> pk = (List<Object>) mutationKeyConverter.fromConnectData(mutationKey.getNativeObject());
                    // ensure the schema is the one used when building the struct.
                    final ConverterAndQuery<Converter> converterAndQueryFinal = this.valueConverterAndQuery;

                    CompletableFuture<KeyValue<Object, Object>> queryResult = new CompletableFuture<>();
                    // we have to process sequentially the records from the same key
                    // otherwise our mutation cache will not be enough efficient
                    // in deduplicating mutations coming from different nodes
                    queryExecutor.executeOrdered(msg.getKey(), () -> {
                        try {
                            if (mutationCache.isMutationProcessed(msg.getKey(), mutationValue.getMd5Digest())) {
                                if (log.isDebugEnabled()) {
                                    log.debug("Message key={} md5={} already processed", msg.getKey(), mutationValue.getMd5Digest());
                                }
                                // ignore duplicated mutation
                                consumer.acknowledge(msg);
                                queryResult.complete(null);
                                CacheStats cacheStats = mutationCache.stats();
                                sourceContext.recordMetric(CACHE_HITS, cacheStats.hitCount());
                                sourceContext.recordMetric(CACHE_MISSES, cacheStats.missCount());
                                sourceContext.recordMetric(CACHE_EVICTIONS, cacheStats.evictionCount());
                                sourceContext.recordMetric(CACHE_SIZE, mutationCache.estimatedSize());
                                sourceContext.recordMetric(QUERY_LATENCY, 0);
                                sourceContext.recordMetric(QUERY_EXECUTORS, config.getQueryExecutors());
                                if (msg.hasProperty(Constants.WRITETIME))
                                    sourceContext.recordMetric(REPLICATION_LATENCY, System.currentTimeMillis() - (Long.parseLong(msg.getProperty(Constants.WRITETIME)) / 1000L));
                                return;
                            }

                            List<Object> nonNullPkValues = pk.stream().filter(e -> e != null).collect(Collectors.toList());
                            long start = System.currentTimeMillis();
                            Tuple3<Row, ConsistencyLevel, UUID> tuple = cassandraClient.selectRow(
                                    nonNullPkValues,
                                    mutationValue.getNodeId(),
                                    Lists.newArrayList(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_ONE),
                                    getSelectStatement(converterAndQueryFinal, nonNullPkValues.size()),
                                    mutationValue.getMd5Digest());
                            CacheStats cacheStats = mutationCache.stats();
                            sourceContext.recordMetric(CACHE_HITS, cacheStats.hitCount());
                            sourceContext.recordMetric(CACHE_MISSES, cacheStats.missCount());
                            sourceContext.recordMetric(CACHE_EVICTIONS, cacheStats.evictionCount());
                            sourceContext.recordMetric(CACHE_SIZE, mutationCache.estimatedSize());
                            long end = System.currentTimeMillis();
                            sourceContext.recordMetric(QUERY_LATENCY, end - start);
                            sourceContext.recordMetric(QUERY_EXECUTORS, config.getQueryExecutors());
                            if (msg.hasProperty(Constants.WRITETIME))
                                sourceContext.recordMetric(REPLICATION_LATENCY, end - (Long.parseLong(msg.getProperty(Constants.WRITETIME)) / 1000L));
                            Object value = tuple._1 == null ? this.emptyValue : converterAndQueryFinal.getConverter().toConnectData(tuple._1);
                            if (ConsistencyLevel.LOCAL_QUORUM.equals(tuple._2()) &&
                                    (!config.getCacheOnlyIfCoordinatorMatch() || (tuple._3 != null && tuple._3.equals(mutationValue.getNodeId())))) {
                                if (log.isDebugEnabled()) {
                                    log.debug("Caching mutation key={} md5={} pk={}", msg.getKey(), mutationValue.getMd5Digest(), nonNullPkValues);
                                }
                                // cache the mutation digest if the coordinator is the source of this event.
                                mutationCache.addMutationMd5(msg.getKey(), mutationValue.getMd5Digest());
                            } else {
                                if (log.isDebugEnabled()) {
                                    log.debug("Not caching mutation key={} md5={} pk={} CL={} coordinator={}",
                                            msg.getKey(), mutationValue.getMd5Digest(), nonNullPkValues, tuple._2(), tuple._3());
                                }
                            }
                            Object key = config.isAvroOutputFormat() ? msg.getKeyBytes() : keyConverter.fromConnectData(mutationKey.getNativeObject());
                            queryResult.complete(new KeyValue(key, value));
                        } catch (Throwable err) {
                            queryResult.completeExceptionally(err);
                        }
                    });
                    final CassandraRecord record = createRecord(converterAndQueryFinal, queryResult, msg);
                    newRecords.add(record);
                }
                Preconditions.checkState(!newRecords.isEmpty(), "Buffer cannot be empty here");
                List<CassandraRecord> usefulRecords = new ArrayList<>(newRecords.size());
                int cacheHits = 0;
                long start = System.currentTimeMillis();
                // wait for all queries to complete
                for (CassandraRecord record : newRecords) {
                    KeyValue res = record.getQueryResult().join();
                    if (res != null) {
                        // if the result is "null" the mutation has been discarded
                        usefulRecords.add(record);
                    } else {
                        cacheHits++;
                    }
                }
                long duration = System.currentTimeMillis() - start;
                long throughput = duration > 0 ? (1000L * newRecords.size()) / duration : 0;
                if (log.isDebugEnabled()) {
                    log.debug("Query time for {} msg in {} ms throughput={} msg/s cacheHits={}", newRecords.size(), duration, throughput, cacheHits);
                }
                consecutiveUnavailableException = 0;
                return usefulRecords;
            } catch (Exception e) {
                Throwable cause = e instanceof CompletionException && e.getCause() != null ? e.getCause() : e;
                if (cause instanceof ExecutionException && cause.getCause() != null) cause = cause.getCause();
                log.warn("Error processing batch, will retry:", cause);
                consecutiveUnavailableException = SourceUtil.backoffRetry(cause, consecutiveUnavailableException, config);
            }
        }
    }

    // TODO: schema evolution. Unlike the Kafka Connect connector, records here carry a real
    // Pulsar Schema<V> (see MyKVRecord.getValueSchema() below), so Pulsar's own broker-side
    // schema registry does version the data topic's schema on each table alteration - there is
    // no "raw bytes with no registry" gap. But this method still swaps the converter/schema in
    // place with no compatibility check of its own: whether an incompatible change (e.g. a
    // column type change) is accepted, versioned, or rejected depends entirely on the data
    // topic's configured SchemaCompatibilityStrategy, not on anything the connector itself
    // verifies before publishing. A downstream consumer can still break if that topic-level
    // strategy allows more than the consumer can handle.

    @Override
    public String getKeyspaceName() { return config.getKeyspaceName(); }

    @Override
    public String getTableName() { return config.getTableName(); }

    @Override
    public CassandraClient getCassandraClient() { return cassandraClient; }

    private interface CassandraRecord extends KVRecord {
        /**
         * @return a Message container the mutation as received from the events topic.
         */
        Message<KeyValue<GenericRecord, MutationValue>> getMutationMessage();

        /**
         * @return a future tracking the result of the Cassandra query triggered by the mutations recorded in the
         * events topic.
         */
        CompletableFuture<KeyValue<Object, Object>> getQueryResult();
    }

    private class MyKVRecord implements CassandraRecord {
        private final ConverterAndQuery<Converter> converterAndQueryFinal;
        private final CompletableFuture<KeyValue<Object, Object>> keyValue;
        private final Message<KeyValue<GenericRecord, MutationValue>> msg;

        public MyKVRecord(ConverterAndQuery<Converter> converterAndQueryFinal, CompletableFuture<KeyValue<Object, Object>> keyValue, Message<KeyValue<GenericRecord, MutationValue>> msg) {
            this.converterAndQueryFinal = converterAndQueryFinal;
            this.keyValue = keyValue;
            this.msg = msg;
        }

        @Override
        public Message<KeyValue<GenericRecord, MutationValue>> getMutationMessage() {
            return msg;
        }

        @Override
        public CompletableFuture<KeyValue<Object, Object>> getQueryResult() {
            return this.keyValue;
        }

        @Override
        public Schema getKeySchema() {
            return keyConverter.getSchema();
        }

        @Override
        public Schema getValueSchema() {
            return converterAndQueryFinal.getConverter().getSchema();
        }

        @Override
        public KeyValueEncodingType getKeyValueEncodingType() {
            return KeyValueEncodingType.SEPARATED;
        }

        @Override
        public KeyValue getValue() {
            // this is guaranteed not to fail
            try {
                return keyValue.get();
            } catch (Exception err) {
                throw new RuntimeException(err);
            }
        }

        @Override
        public Map<String, String> getProperties() {
            return msg.hasProperty(Constants.WRITETIME)
                    ? ImmutableMap.of(Constants.WRITETIME, msg.getProperty(Constants.WRITETIME))
                    : ImmutableMap.of();
        }
    }

    @RequiredArgsConstructor
    private class JsonValueRecord implements CassandraRecord {
        private final MyKVRecord kvRecord;

        @Override
        public byte[] getValue() {
            try {
                return (byte[]) kvRecord.getValue().getValue();
            } catch (Exception err) {
                throw new RuntimeException(err);
            }
        }

        @Override
        public Schema getSchema() {
            return kvRecord.getValueSchema();
        }

        @Override
        public Optional<String> getKey() {
            Object key = kvRecord.getValue().getKey();
            if (!(key instanceof byte[])) {
                throw new IllegalStateException("Invalid key type " + key.getClass().getName());
            }

            // returns a json string in plain text. E.g.: key:[{"a":"38878"}]
            return Optional.of(new String((byte[])key, StandardCharsets.UTF_8));
        }

        @Override
        public Message<KeyValue<GenericRecord, MutationValue>> getMutationMessage() {
            return kvRecord.getMutationMessage();
        }

        @Override
        public CompletableFuture<KeyValue<Object, Object>> getQueryResult() {
            return kvRecord.keyValue;
        }

        @Override
        public Schema getKeySchema() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Schema getValueSchema() {
            throw new UnsupportedOperationException();
        }

        @Override
        public KeyValueEncodingType getKeyValueEncodingType() {
            throw new UnsupportedOperationException();
        }
    }
}
