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

import com.datastax.oss.cdc.CqlLogicalTypes;
import com.datastax.oss.cdc.MutationValue;
import com.datastax.oss.cdc.CassandraClient;
import com.datastax.oss.cdc.CassandraSourceConnectorConfig;
import com.datastax.oss.cdc.MutationCache;
import com.datastax.oss.cdc.Version;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.*;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.pulsar.source.converters.NativeAvroConverter;
import com.datastax.oss.cdc.Constants;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.datastax.oss.cdc.ConfigUtil;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.vavr.Tuple2;
import io.vavr.Tuple3;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Conversions;
import org.apache.avro.specific.SpecificData;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.functions.api.KVRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Source;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
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
public class CassandraSource implements Source<GenericRecord>, SchemaChangeListener {

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
    CassandraClient cassandraClient;
    Consumer<KeyValue<GenericRecord, MutationValue>> consumer = null;

    String dirtyTopicName;
    Converter mutationKeyConverter;
    Converter keyConverter;
    List<String> pkColumns;

    Optional<Pattern> columnPattern = Optional.empty();

    MutationCache<String> mutationCache;

    Schema<KeyValue<GenericRecord, MutationValue>> eventsSchema = Schema.KeyValue(
            Schema.AUTO_CONSUME(),
            Schema.AVRO(MutationValue.class),
            KeyValueEncodingType.SEPARATED);

    /**
     * Converter and CQL query parameters updated on CQL schema update.
     */
    volatile ConverterAndQuery valueConverterAndQuery;


    /**
     * Single threaded executors to fetch CQL rows.
     * Protect from a race condition issue when processing the same PK in parallel.
     *
     * The number of threads is adaptive to avoid overloading the source C* cluster,
     * it depends ont the average query latency and timeouts.
     *
     */
    List<ExecutorService> queryExecutors;

    /**
     *  Per batch total CQL latency
     */
    AtomicLong batchTotalLatency = new AtomicLong(0);

    /**
     *  Per batch total CQL queries
     */
    AtomicLong batchTotalQuery = new AtomicLong(0);

    /**
     * Circular array of the last batch avg latencies use to compute the mobile average query latency.
     */
    long[] batchAvgLatencyList = new long[10];
    int    batchAvgLatencyHead = 0;
    int    batchAvgLatencyListSize = 0;

    /**
     * Number of consecutive unavailableException used to compute the exponential backoff.
     */
    long consecutiveUnavailableException = 0;

    private ArrayBlockingQueue<MyKVRecord> buffer;

    public CassandraSource() {
        // register AVRO logical types conversion
        SpecificData.get().addLogicalTypeConversion(new CqlLogicalTypes.CqlVarintConversion());
        SpecificData.get().addLogicalTypeConversion(new CqlLogicalTypes.CqlDecimalConversion());
        SpecificData.get().addLogicalTypeConversion(new NativeAvroConverter.CqlDurationConversion());
        SpecificData.get().addLogicalTypeConversion(new Conversions.UUIDConversion());
    }
    
    private <T> Future<T> executeOrdered(Object key, Callable<T> task) {
        Preconditions.checkArgument(key != null, "message key should not be null");
        Preconditions.checkState(queryExecutors != null, "queryExecutors should not be null");
        int threadIdx = Math.abs(Objects.hashCode(key)) % queryExecutors.size();
        log.debug("Submit task key={} on thread={}/{}", key, threadIdx, queryExecutors.size());
        return queryExecutors.get(threadIdx).submit(task);
    }

    /**
     * Adjust the number of threads depending on the mobile moving average of the read latency.
     */
    private void adjustExecutors() {
        long batchAvgLatency = this.batchTotalLatency.get() / this.batchTotalQuery.get();
        this.batchAvgLatencyList[this.batchAvgLatencyHead] = batchAvgLatency;
        this.batchAvgLatencyHead = (this.batchAvgLatencyHead + 1) % this.batchAvgLatencyList.length;
        this.batchAvgLatencyListSize = Math.min(batchAvgLatencyListSize + 1, this.batchAvgLatencyList.length);

        long latencyTotal = 0;
        for(int i = 0; i < this.batchAvgLatencyListSize; i++) {
            log.info("batchAvgLatencyList={}, batchAvgLatencyHead={}, batchAvgLatencyListSize={}, i={}",
                    Arrays.toString(batchAvgLatencyList),  batchAvgLatencyHead, batchAvgLatencyListSize, i);
            latencyTotal += this.batchAvgLatencyList[i];
        }
        long mobileAvgLatency = latencyTotal / batchAvgLatencyListSize;
        log.debug("mobileAvgLatency={}, batchAvgLatencyList={}", mobileAvgLatency, Arrays.toString(batchAvgLatencyList));
        if (mobileAvgLatency < config.getQueryMinMobileAvgLatency() && queryExecutors.size() < config.getQueryExecutors() ) {
            queryExecutors.add(Executors.newSingleThreadExecutor());
            log.info("mobileAvgLatency={}, increasing the query executor to {} threads", mobileAvgLatency, queryExecutors.size());
        }
        if (mobileAvgLatency > config.getQueryMaxMobileAvgLatency() && queryExecutors.size() > 1) {
            queryExecutors.remove(queryExecutors.size() - 1).shutdown();
            log.info("mobileAvgLatency={}, decreasing the query executor to {} threads", mobileAvgLatency, queryExecutors.size());
        }
    }

    /**
     * Decrease the number of thread by 10 percent because of the provided Exception.
     * @param throwable
     */
    private void decreaseExecutors(Throwable throwable) {
        if (queryExecutors.size() > 1) {
            int numberOfThreadToRemove = Math.max(1, queryExecutors.size() / 10);
            for(int i = 0; i < numberOfThreadToRemove; i++)
                queryExecutors.remove(queryExecutors.size() - 1).shutdown();
            log.warn("CQL read issue={}, decreasing the query executor to {} threads", throwable, queryExecutors.size());
        } else {
            log.warn("CQL read issue={} with only 1 executor threads, please consider limiting the source connector throughput to avoid overloading the Cassandra cluster");
        }
    }

    private long waitInMs(long attempt) {
        return Math.min(config.getQueryMaxBackoffInSec() * 1000, config.getQueryBackoffInMs() << attempt);
    }

    private long randomWaitInMs(long attempt) {
        return ThreadLocalRandom.current().nextLong(0, waitInMs(attempt));
    }

    private void backoffRetry(Throwable throwable) {
        consecutiveUnavailableException++;
        long pauseInMs = randomWaitInMs(consecutiveUnavailableException);
        log.warn("CQL availability issue={}, consecutiveUnavailableException={}, pausing {}ms before retrying",
                throwable, consecutiveUnavailableException, pauseInMs);
        try {
            Thread.sleep(pauseInMs);
        } catch (InterruptedException ex) {
            log.warn("sleep interrupted:", ex);
        }
    }

    private void initQueryExecutors() {
        log.info("initQueryExecutors with {} treads", this.config.getQueryExecutors());
        this.queryExecutors = new ArrayList<>(this.config.getQueryExecutors());
        for(int i = 0; i < this.config.getQueryExecutors(); i++)
            this.queryExecutors.add(Executors.newSingleThreadExecutor());
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
            log.info("Starting source connector topic={} subscription={} query.executors={}",
                    dirtyTopicName,
                    this.config.getEventsSubscriptionName(),
                    this.config.getQueryExecutors());
        } catch (Throwable err) {
            log.error("Cannot open the connector:", err);
            throw new RuntimeException(err);
        }
    }

    void maybeInitCassandraClient() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, InstantiationException {
        if (this.cassandraClient == null) {
            synchronized (this) {
                if (this.cassandraClient == null) {
                    initCassandraClient();
                }
            }
        }
    }

    void initCassandraClient() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, InstantiationException {
        this.cassandraClient = new CassandraClient(this.config, Version.getVersion(), sourceContext.getSourceName(), this);
        Tuple2<KeyspaceMetadata, TableMetadata> tuple = cassandraClient.getTableMetadata(this.config.getKeyspaceName(), this.config.getTableName());
        Preconditions.checkArgument(tuple._1 != null, String.format(Locale.ROOT, "Keyspace %s does not exist", this.config.getKeyspaceName()));
        Preconditions.checkArgument(tuple._2 != null, String.format(Locale.ROOT, "Table %s.%s does not exist", this.config.getKeyspaceName(), this.config.getTableName()));
        this.pkColumns = tuple._2.getPrimaryKey().stream().map(c -> c.getName().asInternal()).collect(Collectors.toList());
        this.keyConverter = createConverter(getKeyConverterClass(), tuple._1, tuple._2, tuple._2.getPrimaryKey());
        this.mutationKeyConverter = new NativeAvroConverter(tuple._1, tuple._2, tuple._2.getPrimaryKey());
        setValueConverterAndQuery(tuple._1, tuple._2);
    }

    synchronized void setValueConverterAndQuery(KeyspaceMetadata ksm, TableMetadata tableMetadata) {
        try {
            List<ColumnMetadata> columns = tableMetadata.getColumns().values().stream()
                    .filter(c -> !tableMetadata.getPrimaryKey().contains(c))
                    .filter(c -> !columnPattern.isPresent() || columnPattern.get().matcher(c.getName().asInternal()).matches())
                    .collect(Collectors.toList());
            List<ColumnMetadata> staticColumns = tableMetadata.getColumns().values().stream()
                    .filter(c -> c.isStatic())
                    .filter(c -> !tableMetadata.getPrimaryKey().contains(c))
                    .filter(c -> !columnPattern.isPresent() || columnPattern.get().matcher(c.getName().asInternal()).matches())
                    .collect(Collectors.toList());
            log.info("Schema update for table {}.{} replicated columns={}", ksm.getName(), tableMetadata.getName(),
                    columns.stream().map(c -> c.getName().asInternal()).collect(Collectors.toList()));
            this.valueConverterAndQuery = new ConverterAndQuery(
                    tableMetadata.getKeyspace().asInternal(),
                    tableMetadata.getName().asInternal(),
                    createConverter(getValueConverterClass(), ksm, tableMetadata, columns),
                    cassandraClient.buildProjectionClause(columns),
                    cassandraClient.buildProjectionClause(staticColumns),
                    cassandraClient.buildPrimaryKeyClause(tableMetadata),
                    new ConcurrentHashMap<>());
            log.debug("valueConverterAndQuery={}", this.valueConverterAndQuery);
        } catch (Exception e) {
            log.error("Unexpected error", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Build the CQL prepared statement for the specified where clause length.
     * NOTE: The prepared statement cannot be build from the schema listener thread to avoid a possible deadlock.
     * @param valueConverterAndQuery
     * @param whereClauseLength the number of columns in the where clause
     * @return preparedStatement
     */
    synchronized PreparedStatement getSelectStatement(ConverterAndQuery valueConverterAndQuery, int whereClauseLength) {
        return valueConverterAndQuery.getPreparedStatements().computeIfAbsent(whereClauseLength, k ->
                cassandraClient.prepareSelect(
                        valueConverterAndQuery.keyspaceName,
                        valueConverterAndQuery.tableName,
                        valueConverterAndQuery.getProjectionClause(whereClauseLength),
                        valueConverterAndQuery.primaryKeyClause,
                        k));
    }

    Class<?> getKeyConverterClass() {
        return this.config.getKeyConverterClass() == null
                ? NativeAvroConverter.class
                : this.config.getKeyConverterClass();
    }

    Class<?> getValueConverterClass() {
        return this.config.getValueConverterClass() == null
                ? NativeAvroConverter.class
                : this.config.getValueConverterClass();
    }

    Converter createConverter(Class<?> converterClass, KeyspaceMetadata ksm, TableMetadata tableMetadata, List<ColumnMetadata> columns)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        return (Converter) converterClass
                .getDeclaredConstructor(KeyspaceMetadata.class, TableMetadata.class, List.class)
                .newInstance(ksm, tableMetadata, columns);
    }

    @Override
    public void close() {
        log.info("Closing connector");
        if (this.cassandraClient != null) {
            this.cassandraClient.close();
            this.cassandraClient = null;
        }
        if (queryExecutors != null) {
            for (ExecutorService thread : queryExecutors) {
                thread.shutdownNow();
            }
            queryExecutors = null;
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
    public Record<GenericRecord> read() throws Exception
    {
        Preconditions.checkState(this.sourceContext != null, "sourceContext should not be null");
        MyKVRecord myKVRecord = buffer.poll();
        if (myKVRecord != null) {
            consumer.acknowledge(myKVRecord.msg);
            return (Record) myKVRecord;
        }
        // this methods returns only if the buffer holds at least one record
        maybeBatchRead();
        myKVRecord = buffer.poll();
        consumer.acknowledge(myKVRecord.msg);
        return myKVRecord;
    }

    private void maybeBatchRead() throws Exception {
        Preconditions.checkState(buffer.isEmpty(), "Buffer is not empty");
        List<MyKVRecord> newRecords = batchRead();
        while (newRecords.isEmpty()) {
            newRecords = batchRead();
        }
        buffer.addAll(newRecords);
    }

    @SuppressWarnings("unchecked")
    private List<MyKVRecord> batchRead() throws Exception {
        batchTotalLatency.set(0);
        batchTotalQuery.set(0);
        List<MyKVRecord> newRecords = new ArrayList<>();
        if (this.queryExecutors == null)
            initQueryExecutors();
        try {
            maybeInitCassandraClient();

            // we want to fill the buffer
            // this method will block until we receive at least one record
            while (newRecords.size() < this.config.getBatchSize()) {
                final Message<KeyValue<GenericRecord, MutationValue>> msg = consumer.receive(1, TimeUnit.SECONDS);
                if (msg == null) {
                    if (!newRecords.isEmpty()) {
                        log.debug("no message received, buffer size {}", newRecords.size());
                        // no more records within the timeout, but we have at least one record
                        break;
                    } else {
                        log.debug("no message received, buffer size {}", newRecords.size());
                        continue;
                    }
                }
                final KeyValue<GenericRecord, MutationValue> kv = msg.getValue();
                final GenericRecord mutationKey = kv.getKey();
                final MutationValue mutationValue = kv.getValue();

                log.debug("Message from producer={} msgId={} key={} value={} schema {}\n",
                        msg.getProducerName(), msg.getMessageId(), kv.getKey(), kv.getValue(), msg.getReaderSchema().orElse(null));

                List<Object> pk = (List<Object>) mutationKeyConverter.fromConnectData(mutationKey.getNativeObject());
                // ensure the schema is the one used when building the struct.
                final ConverterAndQuery converterAndQueryFinal = this.valueConverterAndQuery;

                CompletableFuture<KeyValue<Object, Object>> queryResult = new CompletableFuture<>();
                // we have to process sequentially the records from the same key
                // otherwise our mutation cache will not be enough efficient
                // in deduplicating mutations coming from different nodes
                executeOrdered(msg.getKey(), () -> {
                    try {
                        if (mutationCache.isMutationProcessed(msg.getKey(), mutationValue.getMd5Digest())) {
                            log.debug("Message key={} md5={} already processed", msg.getKey(), mutationValue.getMd5Digest());
                            // ignore duplicated mutation
                            consumer.acknowledge(msg);
                            queryResult.complete(null);
                            CacheStats cacheStats = mutationCache.stats();
                            sourceContext.recordMetric(CACHE_HITS, cacheStats.hitCount());
                            sourceContext.recordMetric(CACHE_MISSES, cacheStats.missCount());
                            sourceContext.recordMetric(CACHE_EVICTIONS, cacheStats.evictionCount());
                            sourceContext.recordMetric(CACHE_SIZE, mutationCache.estimatedSize());
                            sourceContext.recordMetric(QUERY_LATENCY, 0);
                            sourceContext.recordMetric(QUERY_EXECUTORS, queryExecutors.size());
                            if (msg.getProperty(Constants.WRITETIME) != null)
                                sourceContext.recordMetric(REPLICATION_LATENCY, System.currentTimeMillis() - (Long.parseLong(msg.getProperty(Constants.WRITETIME)) / 1000));
                            return null;
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
                        sourceContext.recordMetric(QUERY_EXECUTORS, queryExecutors.size());
                        batchTotalLatency.addAndGet(end - start);
                        batchTotalQuery.incrementAndGet();
                        if (msg.getProperty(Constants.WRITETIME) != null)
                            sourceContext.recordMetric(REPLICATION_LATENCY, end - (Long.parseLong(msg.getProperty(Constants.WRITETIME)) / 1000));
                        Object value = tuple._1 == null ? null : converterAndQueryFinal.getConverter().toConnectData(tuple._1);
                        if (ConsistencyLevel.LOCAL_QUORUM.equals(tuple._2()) &&
                                (!config.getCacheOnlyIfCoordinatorMatch() || (tuple._3 != null && tuple._3.equals(mutationValue.getNodeId())))) {
                            log.debug("Caching mutation key={} md5={} pk={}", msg.getKey(), mutationValue.getMd5Digest(), nonNullPkValues);
                            // cache the mutation digest if the coordinator is the source of this event.
                            mutationCache.addMutationMd5(msg.getKey(), mutationValue.getMd5Digest());
                        } else {
                            log.debug("Not caching mutation key={} md5={} pk={} CL={} coordinator={}",
                                    msg.getKey(), mutationValue.getMd5Digest(), nonNullPkValues, tuple._2(), tuple._3());
                        }
                        queryResult.complete(new KeyValue(msg.getKeyBytes(), value));
                    } catch (Throwable err) {
                        queryResult.completeExceptionally(err);
                    }
                    return null;
                });
                final MyKVRecord record = new MyKVRecord(converterAndQueryFinal, queryResult, msg);
                newRecords.add(record);
            }
            Preconditions.checkState(!newRecords.isEmpty(), "Buffer cannot be empty here");
            List<MyKVRecord> usefulRecords = new ArrayList<>(newRecords.size());
            int cacheHits = 0;
            long start = System.currentTimeMillis();
            // wait for all queries to complete
            for (MyKVRecord record : newRecords) {
                KeyValue res = record.keyValue.join();
                if (res != null) {
                    // if the result is "null" the mutation has been discarded
                    usefulRecords.add(record);
                } else {
                    cacheHits++;
                }
            }
            long duration = System.currentTimeMillis() - start;
            long throughput = duration > 0 ? (1000 * newRecords.size()) / duration : 0;
            log.debug("Query time for {} msg in {} ms throughput={} msg/s cacheHits={}", newRecords.size(), duration, throughput, cacheHits);
            if (batchTotalQuery.get() > 0) {
                adjustExecutors();
            }
            consecutiveUnavailableException = 0;
            return usefulRecords;
        } catch (CompletionException e) {
            Throwable e2 = e.getCause();
            if (e2 instanceof ExecutionException) {
                e2 = e2.getCause();
            }
            log.info("CompletionException cause:", e2);

            if (e2 instanceof com.datastax.oss.driver.api.core.servererrors.ReadTimeoutException ||
                    e2 instanceof com.datastax.oss.driver.api.core.servererrors.OverloadedException) {
                decreaseExecutors(e2);
            } else if (e2 instanceof com.datastax.oss.driver.api.core.AllNodesFailedException) {
                // just retry
            } else {
                log.warn("Unexpected exception class=" + e.getClass() + " message=" + e.getMessage() + " cause={}" + e.getCause(), e);
                throw e;
            }

            for (MyKVRecord record : newRecords) {
                negativeAcknowledge(consumer, record.getMsg()); // fail every message in the buffer
            }
            backoffRetry(e2);
            return Collections.emptyList();
        } catch(com.datastax.oss.driver.api.core.AllNodesFailedException e) {
            log.info("AllNodesFailedException:", e);
            for (MyKVRecord record : newRecords) {
                negativeAcknowledge(consumer, record.getMsg()); // fail every message in the buffer
            }
            backoffRetry(e);
            return Collections.emptyList();
        } catch(Throwable e) {
            log.error("Unrecoverable error:", e);
            for (MyKVRecord record : newRecords) {
                negativeAcknowledge(consumer, record.getMsg());
            }
            throw e;
        }
    }

    void negativeAcknowledge(final Consumer<KeyValue<GenericRecord, MutationValue>> consumer,
                             final Message<KeyValue<GenericRecord, MutationValue>> message) {
        consumer.negativeAcknowledge(message);
    }

    @Override
    public void onKeyspaceCreated(@NonNull KeyspaceMetadata keyspace) {

    }

    @Override
    public void onKeyspaceDropped(@NonNull KeyspaceMetadata keyspace) {

    }

    @Override
    public void onKeyspaceUpdated(@NonNull KeyspaceMetadata current, @NonNull KeyspaceMetadata previous) {

    }

    @Override
    public void onTableCreated(@NonNull TableMetadata table) {

    }

    @Override
    public void onTableDropped(@NonNull TableMetadata table) {

    }

    @SneakyThrows
    @Override
    public void onTableUpdated(@NonNull TableMetadata current, @NonNull TableMetadata previous) {
        log.debug("onTableUpdated {} {}", current, previous);
        if (current.getKeyspace().asInternal().equals(config.getKeyspaceName())
                && current.getName().asInternal().equals(config.getTableName())) {
            KeyspaceMetadata ksm = cassandraClient.getCqlSession().getMetadata().getKeyspace(current.getKeyspace()).get();
            setValueConverterAndQuery(ksm, current);
        }
    }

    @SneakyThrows
    @Override
    public void onUserDefinedTypeCreated(@NonNull UserDefinedType type) {
        log.debug("onUserDefinedTypeCreated {}", type);
        if (type.getKeyspace().asInternal().equals(config.getKeyspaceName())) {
            KeyspaceMetadata ksm = cassandraClient.getCqlSession().getMetadata().getKeyspace(type.getKeyspace()).get();
            setValueConverterAndQuery(ksm, ksm.getTable(config.getTableName()).get());
        }
    }

    @Override
    public void onUserDefinedTypeDropped(@NonNull UserDefinedType type) {
        log.debug("onUserDefinedTypeDropped {}", type);
    }

    @SneakyThrows
    @Override
    public void onUserDefinedTypeUpdated(@NonNull UserDefinedType userDefinedType, @NonNull UserDefinedType userDefinedType1) {
        log.debug("onUserDefinedTypeUpdated {} {}", userDefinedType, userDefinedType1);
        if (userDefinedType.getKeyspace().asCql(true).equals(config.getKeyspaceName())) {
            KeyspaceMetadata ksm = cassandraClient.getCqlSession().getMetadata().getKeyspace(userDefinedType.getKeyspace()).get();
            setValueConverterAndQuery(ksm, ksm.getTable(config.getTableName()).get());
        }
    }

    @Override
    public void onFunctionCreated(@NonNull FunctionMetadata function) {

    }

    @Override
    public void onFunctionDropped(@NonNull FunctionMetadata function) {

    }

    @Override
    public void onFunctionUpdated(@NonNull FunctionMetadata current, @NonNull FunctionMetadata previous) {

    }

    @Override
    public void onAggregateCreated(@NonNull AggregateMetadata aggregate) {

    }

    @Override
    public void onAggregateDropped(@NonNull AggregateMetadata aggregate) {

    }

    @Override
    public void onAggregateUpdated(@NonNull AggregateMetadata current, @NonNull AggregateMetadata previous) {

    }

    @Override
    public void onViewCreated(@NonNull ViewMetadata view) {

    }

    @Override
    public void onViewDropped(@NonNull ViewMetadata view) {

    }

    @Override
    public void onViewUpdated(@NonNull ViewMetadata current, @NonNull ViewMetadata previous) {

    }

    private class MyKVRecord implements KVRecord {
        private final ConverterAndQuery converterAndQueryFinal;
        private final CompletableFuture<KeyValue<Object, Object>> keyValue;
        private final Message<KeyValue<GenericRecord, MutationValue>> msg;

        public MyKVRecord(ConverterAndQuery converterAndQueryFinal, CompletableFuture<KeyValue<Object, Object>> keyValue, Message<KeyValue<GenericRecord, MutationValue>> msg) {
            this.converterAndQueryFinal = converterAndQueryFinal;
            this.keyValue = keyValue;
            this.msg = msg;
        }

        public Message<KeyValue<GenericRecord, MutationValue>> getMsg() {
            return msg;
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
            return ImmutableMap.of(Constants.WRITETIME, msg.getProperty(Constants.WRITETIME));
        }
    }
}
