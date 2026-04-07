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
import com.datastax.oss.cdc.ConfigUtil;
import com.datastax.oss.cdc.Constants;
import com.datastax.oss.cdc.CqlLogicalTypes;
import com.datastax.oss.cdc.MutationCache;
import com.datastax.oss.cdc.MutationValue;
import com.datastax.oss.cdc.Version;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.AggregateMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.ViewMetadata;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.pulsar.source.converters.NativeAvroConverter;
import com.datastax.oss.pulsar.source.converters.NativeJsonConverter;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.vavr.Tuple2;
import io.vavr.Tuple3;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Conversions;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.functions.api.KVRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Source;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

// Messaging abstraction layer imports
import com.datastax.oss.cdc.messaging.MessagingClient;
import com.datastax.oss.cdc.messaging.MessageConsumer;
import com.datastax.oss.cdc.messaging.Message;
import com.datastax.oss.cdc.messaging.MessagingException;
import com.datastax.oss.cdc.messaging.config.ClientConfig;
import com.datastax.oss.cdc.messaging.config.ConsumerConfig;
import com.datastax.oss.cdc.messaging.config.InitialPosition;
import com.datastax.oss.cdc.messaging.config.MessagingProvider;
import com.datastax.oss.cdc.messaging.config.SubscriptionType;
import com.datastax.oss.cdc.messaging.config.impl.ClientConfigBuilder;
import com.datastax.oss.cdc.messaging.config.impl.ConsumerConfigBuilder;
import com.datastax.oss.cdc.messaging.factory.MessagingClientFactory;
import com.datastax.oss.cdc.messaging.schema.SchemaDefinition;
import com.datastax.oss.cdc.messaging.schema.SchemaType;
import com.datastax.oss.cdc.messaging.schema.impl.BaseSchemaDefinition;
import com.datastax.oss.cdc.NativeSchemaWrapper;

import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
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
    MessageConsumer<GenericRecord, MutationValue> consumer = null;
    MessagingClient messagingClient = null;
    volatile CassandraClient cassandraClient;

    String dirtyTopicName;
    Converter mutationKeyConverter;
    Converter keyConverter;

    Optional<Pattern> columnPattern = Optional.empty();

    MutationCache<String> mutationCache;

    // Schema definitions for messaging abstraction
    SchemaDefinition keySchemaDefinition;
    SchemaDefinition valueSchemaDefinition;

    /**
     * Converter and CQL query parameters updated on CQL schema update.
     */
    volatile ConverterAndQuery valueConverterAndQuery;

    /**
     * Holds an empty value for use with delete mutations. The empty value life cycle is coupled with the
     * valueConverterAndQuery life cycle and is meant to avoid re-creating empty values for delete mutations.
     */
    private Object emptyValue;

    /**
     * Single threaded executors to fetch CQL rows.
     * Protect from a race condition issue when processing the same PK in parallel.
     * <p>
     * The number of threads is adaptive to avoid overloading the source C* cluster,
     * it depends ont the average query latency and timeouts.
     */
    List<ExecutorService> queryExecutors;

    /**
     * Per batch total CQL latency
     */
    final AtomicLong batchTotalLatency = new AtomicLong(0);

    /**
     * Per batch total CQL queries
     */
    final AtomicLong batchTotalQuery = new AtomicLong(0);

    /**
     * Circular array of the last batch avg latencies use to compute the mobile average query latency.
     */
    long[] batchAvgLatencyList = new long[10];
    int batchAvgLatencyHead = 0;
    int batchAvgLatencyListSize = 0;

    /**
     * Number of consecutive unavailableException used to compute the exponential backoff.
     */
    long consecutiveUnavailableException = 0;

    private ArrayBlockingQueue<CassandraRecord> buffer;

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
        for (int i = 0; i < this.batchAvgLatencyListSize; i++) {
            log.debug("batchAvgLatencyList={}, batchAvgLatencyHead={}, batchAvgLatencyListSize={}, i={}",
                    Arrays.toString(batchAvgLatencyList), batchAvgLatencyHead, batchAvgLatencyListSize, i);
            latencyTotal += this.batchAvgLatencyList[i];
        }
        long mobileAvgLatency = latencyTotal / batchAvgLatencyListSize;
        log.debug("mobileAvgLatency={}, batchAvgLatencyList={}", mobileAvgLatency, Arrays.toString(batchAvgLatencyList));
        if (mobileAvgLatency < config.getQueryMinMobileAvgLatency() && queryExecutors.size() < config.getQueryExecutors()) {
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
     *
     * @param throwable
     */
    private void decreaseExecutors(Throwable throwable) {
        if (queryExecutors.size() > 1) {
            int numberOfThreadToRemove = Math.max(1, queryExecutors.size() / 10);
            for (int i = 0; i < numberOfThreadToRemove; i++)
                queryExecutors.remove(queryExecutors.size() - 1).shutdown();
            log.warn("CQL read issue={}, decreasing the query executor to {} threads", throwable, queryExecutors.size());
        } else {
            log.warn("CQL read issue={} with only 1 executor threads, please consider limiting the source connector throughput to avoid overloading the Cassandra cluster", throwable);
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
        for (int i = 0; i < this.config.getQueryExecutors(); i++)
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
            
            // Initialize schema definitions for messaging abstraction
            initializeSchemaDefinitions();
            
            // Initialize messaging client and consumer using abstraction layer
            initializeMessagingClient();
            
            // Create consumer using messaging abstraction
            this.consumer = createConsumer();
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
        this.keyConverter = createConverter(getKeyConverterClass(), tuple._1, tuple._2, tuple._2.getPrimaryKey());
        this.mutationKeyConverter = new NativeAvroConverter(tuple._1, tuple._2, tuple._2.getPrimaryKey());
        setValueConverterAndQuery(tuple._1, tuple._2);
    }

    /**
     * Check if the table has only primary key columns.
     * @param tableMetadata the table metadata
     * @return true if the table has only primary key columns, false otherwise
     */
    private boolean isPrimaryKeyOnlyTable(TableMetadata tableMetadata) {
        // if the table has no columns other than the primary key, we can skip the value converter
        return tableMetadata.getColumns().size() == tableMetadata.getPrimaryKey().size() &&
                new HashSet<>(tableMetadata.getPrimaryKey()).containsAll(tableMetadata.getColumns().values());
    }

    synchronized void setValueConverterAndQuery(KeyspaceMetadata ksm, TableMetadata tableMetadata) {
        try {
            boolean isPrimaryKeyOnlyTable = isPrimaryKeyOnlyTable(tableMetadata);
            List<ColumnMetadata> columns = tableMetadata.getColumns().values().stream()
                    // include primary keys in the json only output format options
                    // TODO: PERF: Infuse the key values instead of reading from DB https://github.com/datastax/cdc-apache-cassandra/issues/84
                    // If primary key only table, then add all the columns into the value schema.
                    .filter(c -> config.isJsonOnlyOutputFormat() || isPrimaryKeyOnlyTable || !tableMetadata.getPrimaryKey().contains(c))
                    .filter(c -> !columnPattern.isPresent() || columnPattern.get().matcher(c.getName().asInternal()).matches())
                    .collect(Collectors.toList());
            List<ColumnMetadata> staticColumns = tableMetadata.getColumns().values().stream()
                    .filter(ColumnMetadata::isStatic)
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
    synchronized PreparedStatement getSelectStatement(ConverterAndQuery valueConverterAndQuery, int whereClauseLength) {
        return valueConverterAndQuery.preparedStatements.computeIfAbsent(whereClauseLength, k ->
                cassandraClient.prepareSelect(
                        valueConverterAndQuery.keyspaceName,
                        valueConverterAndQuery.tableName,
                        valueConverterAndQuery.getProjectionClause(whereClauseLength),
                        valueConverterAndQuery.primaryKeyClause,
                        k
                ));
    }

    Class<?> getKeyConverterClass() {
        return this.config.getKeyConverterClass() == null
                ? this.config.isJsonOutputFormat() ? NativeJsonConverter.class : NativeAvroConverter.class
                : this.config.getKeyConverterClass();
    }

    Class<?> getValueConverterClass() {
        return this.config.getValueConverterClass() == null
                ? this.config.isJsonOutputFormat() ? NativeJsonConverter.class : NativeAvroConverter.class
                : this.config.getValueConverterClass();
    }

    Converter createConverter(Class<?> converterClass, KeyspaceMetadata ksm, TableMetadata tableMetadata, List<ColumnMetadata> columns)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        return (Converter) converterClass
                .getDeclaredConstructor(KeyspaceMetadata.class, TableMetadata.class, List.class)
                .newInstance(ksm, tableMetadata, columns);
    }

    CassandraRecord createRecord(ConverterAndQuery converterAndQueryFinal, CompletableFuture<KeyValue<Object, Object>> keyValue, Message<GenericRecord, MutationValue> msg) {
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
        if (this.messagingClient != null) {
            try {
                this.messagingClient.close();
            } catch (MessagingException e) {
                log.warn("Error closing messaging client", e);
            }
            this.messagingClient = null;
        }
        if (queryExecutors != null) {
            for (ExecutorService thread : queryExecutors) {
                thread.shutdownNow();
            }
            queryExecutors = null;
        }
    }

    /**
     * Initialize schema definitions for key and value.
     */
    void initializeSchemaDefinitions() {
        // Create key schema definition using AUTO_CONSUME for GenericRecord
        Schema<GenericRecord> pulsarKeySchema = Schema.AUTO_CONSUME();
        this.keySchemaDefinition = BaseSchemaDefinition.builder()
                .type(SchemaType.AVRO)
                .schemaDefinition("AUTO_CONSUME")
                .nativeSchema(pulsarKeySchema)
                .name("GenericRecord")
                .build();
        
        // Create value schema definition for MutationValue
        Schema<MutationValue> pulsarValueSchema = Schema.AVRO(MutationValue.class);
        this.valueSchemaDefinition = BaseSchemaDefinition.builder()
                .type(SchemaType.AVRO)
                .schemaDefinition("MutationValue")
                .nativeSchema(pulsarValueSchema)
                .name("MutationValue")
                .build();
        
        log.debug("Initialized schema definitions for consumer");
    }

    /**
     * Initialize messaging client using abstraction layer.
     */
    void initializeMessagingClient() throws MessagingException {
        try {
            // Build client configuration - default to Pulsar for backward compatibility
            // The service URL will be obtained from the Pulsar SourceContext's underlying client
            // For now, we use a placeholder that will be resolved by the Pulsar implementation
            ClientConfig clientConfig = ClientConfigBuilder.builder()
                    .provider(MessagingProvider.PULSAR)
                    .serviceUrl("pulsar://localhost:6650") // Placeholder - actual URL from SourceContext
                    .build();
            
            // Create messaging client using factory
            // Note: For Pulsar connector, the actual PulsarClient from SourceContext
            // should be reused. This will be handled in the PulsarMessagingClient implementation.
            this.messagingClient = MessagingClientFactory.create(clientConfig);
            this.messagingClient.initialize(clientConfig);
            
            log.info("Messaging client initialized successfully");
        } catch (Exception e) {
            log.error("Failed to initialize messaging client", e);
            throw new MessagingException("Failed to initialize messaging client", e);
        }
    }

    /**
     * Create consumer using messaging abstraction layer.
     */
    MessageConsumer<GenericRecord, MutationValue> createConsumer() throws MessagingException {
        try {
            // Map subscription type from config string to abstraction enum
            SubscriptionType subscriptionType = mapSubscriptionType(this.config.getEventsSubscriptionType());
            
            // Build consumer configuration
            ConsumerConfig<GenericRecord, MutationValue> consumerConfig =
                    ConsumerConfigBuilder.<GenericRecord, MutationValue>builder()
                    .topic(dirtyTopicName)
                    .subscriptionName(this.config.getEventsSubscriptionName())
                    .subscriptionType(subscriptionType)
                    .initialPosition(InitialPosition.EARLIEST)
                    .consumerName("CDC Consumer")
                    .keySchema(keySchemaDefinition)
                    .valueSchema(valueSchemaDefinition)
                    .receiverQueueSize(1000) // Default Pulsar value
                    .ackTimeoutMs(0) // No ack timeout by default
                    .build();
            
            MessageConsumer<GenericRecord, MutationValue> consumer =
                    messagingClient.createConsumer(consumerConfig);
            
            log.info("Consumer created successfully for topic={} subscription={} type={}",
                    dirtyTopicName, this.config.getEventsSubscriptionName(), subscriptionType);
            
            return consumer;
        } catch (Exception e) {
            log.error("Failed to create consumer", e);
            throw new MessagingException("Failed to create consumer", e);
        }
    }

    /**
     * Map subscription type string to abstraction enum.
     * Package-private for testing.
     */
    SubscriptionType mapSubscriptionType(String subscriptionTypeStr) {
        if (subscriptionTypeStr == null) {
            return SubscriptionType.EXCLUSIVE;
        }
        
        switch (subscriptionTypeStr.toUpperCase()) {
            case "EXCLUSIVE":
                return SubscriptionType.EXCLUSIVE;
            case "SHARED":
                return SubscriptionType.SHARED;
            case "FAILOVER":
                return SubscriptionType.FAILOVER;
            case "KEY_SHARED":
                return SubscriptionType.KEY_SHARED;
            default:
                log.warn("Unknown subscription type: {}, defaulting to EXCLUSIVE", subscriptionTypeStr);
                return SubscriptionType.EXCLUSIVE;
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
        batchTotalLatency.set(0);
        batchTotalQuery.set(0);
        List<CassandraRecord> newRecords = new ArrayList<>();
        if (this.queryExecutors == null)
            initQueryExecutors();
        try {
            maybeInitCassandraClient();

            // we want to fill the buffer
            // this method will block until we receive at least one record
            while (newRecords.size() < this.config.getBatchSize()) {
                final Message<GenericRecord, MutationValue> msg = consumer.receive(Duration.ofSeconds(1));
                if (msg == null) {
                    if (!newRecords.isEmpty()) {
                        log.debug("no message received, buffer size {}", newRecords.size());
                        // no more records within the timeout, but we have at least one record
                        break;
                    } else {
                        log.debug("no message received");
                        continue;
                    }
                }
                final GenericRecord mutationKey = msg.getKey();
                final MutationValue mutationValue = msg.getValue();

                log.debug("Message msgId={} key={} value={}\n",
                        msg.getMessageId(), mutationKey, mutationValue);

                // Handle both GenericAvroRecord and byte array cases
                Object nativeKeyObject = mutationKey.getNativeObject();
                org.apache.avro.generic.GenericRecord avroRecord;
                
                if (nativeKeyObject instanceof org.apache.avro.generic.GenericRecord) {
                    // Direct Avro GenericRecord
                    avroRecord = (org.apache.avro.generic.GenericRecord) nativeKeyObject;
                } else if (nativeKeyObject instanceof byte[]) {
                    // Byte array - need to deserialize to Avro GenericRecord
                    byte[] keyBytes = (byte[]) nativeKeyObject;
                    try {
                        org.apache.avro.io.DatumReader<org.apache.avro.generic.GenericRecord> reader =
                            new org.apache.avro.generic.GenericDatumReader<>(
                                ((NativeAvroConverter) mutationKeyConverter).nativeSchema);
                        org.apache.avro.io.Decoder decoder =
                            org.apache.avro.io.DecoderFactory.get().binaryDecoder(keyBytes, null);
                        avroRecord = reader.read(null, decoder);
                    } catch (Exception e) {
                        log.error("Failed to deserialize key bytes to Avro GenericRecord", e);
                        throw new RuntimeException("Failed to deserialize key", e);
                    }
                } else {
                    throw new IllegalStateException(
                        "Unexpected key type: " + (nativeKeyObject != null ? nativeKeyObject.getClass().getName() : "null") +
                        ". Expected org.apache.avro.generic.GenericRecord or byte[]");
                }
                
                List<Object> pk = (List<Object>) mutationKeyConverter.fromConnectData(avroRecord);
                // ensure the schema is the one used when building the struct.
                final ConverterAndQuery converterAndQueryFinal = this.valueConverterAndQuery;

                CompletableFuture<KeyValue<Object, Object>> queryResult = new CompletableFuture<>();
                // we have to process sequentially the records from the same key
                // otherwise our mutation cache will not be enough efficient
                // in deduplicating mutations coming from different nodes
                executeOrdered(msg.getKey(), () -> {
                    try {
                        String msgKey = msg.getKey().toString();
                        if (mutationCache.isMutationProcessed(msgKey, mutationValue.getMd5Digest())) {
                            log.debug("Message key={} md5={} already processed", msgKey, mutationValue.getMd5Digest());
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
                            msg.getProperty(Constants.WRITETIME).ifPresent(writetime ->
                                sourceContext.recordMetric(REPLICATION_LATENCY, System.currentTimeMillis() - (Long.parseLong(writetime) / 1000L)));
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
                        msg.getProperty(Constants.WRITETIME).ifPresent(writetime ->
                            sourceContext.recordMetric(REPLICATION_LATENCY, end - (Long.parseLong(writetime) / 1000L)));
                        Object value = tuple._1 == null ? this.emptyValue : converterAndQueryFinal.converter.toConnectData(tuple._1);
                        if (ConsistencyLevel.LOCAL_QUORUM.equals(tuple._2()) &&
                                (!config.getCacheOnlyIfCoordinatorMatch() || (tuple._3 != null && tuple._3.equals(mutationValue.getNodeId())))) {
                            log.debug("Caching mutation key={} md5={} pk={}", msgKey, mutationValue.getMd5Digest(), nonNullPkValues);
                            // cache the mutation digest if the coordinator is the source of this event.
                            mutationCache.addMutationMd5(msgKey, mutationValue.getMd5Digest());
                        } else {
                            log.debug("Not caching mutation key={} md5={} pk={} CL={} coordinator={}",
                            msgKey, mutationValue.getMd5Digest(), nonNullPkValues, tuple._2(), tuple._3());
                        }
                        Object key = config.isAvroOutputFormat() ? msg.getKey() : keyConverter.fromConnectData(mutationKey.getNativeObject());
                        queryResult.complete(new KeyValue(key, value));
                    } catch (Throwable err) {
                        queryResult.completeExceptionally(err);
                    }
                    return null;
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

            for (CassandraRecord record : newRecords) {
                negativeAcknowledge(consumer, record.getMutationMessage()); // fail every message in the buffer
            }
            backoffRetry(e2);
            return Collections.emptyList();
        } catch (com.datastax.oss.driver.api.core.AllNodesFailedException e) {
            log.info("AllNodesFailedException:", e);
            for (CassandraRecord record : newRecords) {
                negativeAcknowledge(consumer, record.getMutationMessage()); // fail every message in the buffer
            }
            backoffRetry(e);
            return Collections.emptyList();
        } catch (Throwable e) {
            log.error("Unrecoverable error:", e);
            for (CassandraRecord record : newRecords) {
                negativeAcknowledge(consumer, record.getMutationMessage());
            }
            throw e;
        }
    }

    void negativeAcknowledge(final MessageConsumer<GenericRecord, MutationValue> consumer,
                             final Message<GenericRecord, MutationValue> message) {
        try {
            consumer.negativeAcknowledge(message);
        } catch (MessagingException e) {
            log.error("Error negative acknowledging message", e);
        }
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

    @Override
    public void onTableUpdated(@NonNull TableMetadata current, @NonNull TableMetadata previous) {
        log.debug("onTableUpdated {} {}", current, previous);
        if (current.getKeyspace().asInternal().equals(config.getKeyspaceName())
                && current.getName().asInternal().equals(config.getTableName())) {
            // Use ifPresent to avoid blocking .get() call on driver thread
            cassandraClient.getCqlSession().getMetadata().getKeyspace(current.getKeyspace()).ifPresent(ksm -> {
                try {
                    setValueConverterAndQuery(ksm, current);
                } catch (Exception e) {
                    log.error("Error updating table schema", e);
                }
            });
        }
    }

    @Override
    public void onUserDefinedTypeCreated(@NonNull UserDefinedType type) {
        log.debug("onUserDefinedTypeCreated {}", type);
        if (type.getKeyspace().asInternal().equals(config.getKeyspaceName())) {
            // Use ifPresent to avoid blocking .get() calls on driver thread
            cassandraClient.getCqlSession().getMetadata().getKeyspace(type.getKeyspace()).ifPresent(ksm -> {
                ksm.getTable(config.getTableName()).ifPresent(table -> {
                    try {
                        setValueConverterAndQuery(ksm, table);
                    } catch (Exception e) {
                        log.error("Error updating schema for UDT creation", e);
                    }
                });
            });
        }
    }

    @Override
    public void onUserDefinedTypeDropped(@NonNull UserDefinedType type) {
        log.debug("onUserDefinedTypeDropped {}", type);
    }

    @Override
    public void onUserDefinedTypeUpdated(@NonNull UserDefinedType userDefinedType, @NonNull UserDefinedType userDefinedType1) {
        log.debug("onUserDefinedTypeUpdated {} {}", userDefinedType, userDefinedType1);
        if (userDefinedType.getKeyspace().asCql(true).equals(config.getKeyspaceName())) {
            // Use ifPresent to avoid blocking .get() calls on driver thread
            cassandraClient.getCqlSession().getMetadata().getKeyspace(userDefinedType.getKeyspace()).ifPresent(ksm -> {
                ksm.getTable(config.getTableName()).ifPresent(table -> {
                    try {
                        setValueConverterAndQuery(ksm, table);
                    } catch (Exception e) {
                        log.error("Error updating schema for UDT update", e);
                    }
                });
            });
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

    private interface CassandraRecord extends KVRecord {
        /**
         * @return a Message container the mutation as received from the events topic.
         */
        Message<GenericRecord, MutationValue> getMutationMessage();

        /**
         * @return a future tracking the result of the Cassandra query triggered by the mutations recorded in the
         * events topic.
         */
        CompletableFuture<KeyValue<Object, Object>> getQueryResult();
    }

    private class MyKVRecord implements CassandraRecord {
        private final ConverterAndQuery converterAndQueryFinal;
        private final CompletableFuture<KeyValue<Object, Object>> keyValue;
        private final Message<GenericRecord, MutationValue> msg;

        public MyKVRecord(ConverterAndQuery converterAndQueryFinal, CompletableFuture<KeyValue<Object, Object>> keyValue, Message<GenericRecord, MutationValue> msg) {
            this.converterAndQueryFinal = converterAndQueryFinal;
            this.keyValue = keyValue;
            this.msg = msg;
        }

        @Override
        public Message<GenericRecord, MutationValue> getMutationMessage() {
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
            return converterAndQueryFinal.converter.getSchema();
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
            Map<String, String> props = msg.getProperties();
            if (props != null && props.containsKey(Constants.WRITETIME)) {
                return ImmutableMap.of(Constants.WRITETIME, props.get(Constants.WRITETIME));
            }
            return ImmutableMap.of();
        }
    }

    @RequiredArgsConstructor
    private class JsonValueRecord implements CassandraRecord {
        private final MyKVRecord kvRecord;

        @Override
        public byte[] getValue() {
            try {
                Object value = kvRecord.getValue().getValue();
                // Handle both byte[] (from NativeConverter) and GenericRecord (from GenericConverter)
                if (value instanceof byte[]) {
                    return (byte[]) value;
                } else if (value instanceof org.apache.pulsar.client.api.schema.GenericRecord) {
                    // GenericRecord from Pulsar - need to serialize to bytes
                    org.apache.pulsar.client.api.schema.GenericRecord genericRecord =
                        (org.apache.pulsar.client.api.schema.GenericRecord) value;
                    
                    // Use the converter to serialize the GenericRecord to bytes
                    // The converter's fromConnectData method handles the serialization
                    @SuppressWarnings("unchecked")
                    Converter<byte[], org.apache.pulsar.client.api.schema.GenericRecord, ?, byte[]> converter =
                        (Converter<byte[], org.apache.pulsar.client.api.schema.GenericRecord, ?, byte[]>)
                        kvRecord.converterAndQueryFinal.converter;
                    return converter.fromConnectData(genericRecord);
                } else {
                    throw new IllegalStateException("Unexpected value type: " +
                        (value != null ? value.getClass().getName() : "null") +
                        ". Expected byte[] or GenericRecord");
                }
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
            // Handle both byte[] and GenericRecord for keys
            if (key instanceof byte[]) {
                // returns a json string in plain text. E.g.: key:[{"a":"38878"}]
                return Optional.of(new String((byte[])key, StandardCharsets.UTF_8));
            } else if (key instanceof org.apache.pulsar.client.api.schema.GenericRecord) {
                // GenericRecord from Pulsar - need to serialize to bytes
                org.apache.pulsar.client.api.schema.GenericRecord genericRecord =
                    (org.apache.pulsar.client.api.schema.GenericRecord) key;
                
                try {
                    // Use the key converter to serialize the GenericRecord to bytes
                    @SuppressWarnings("unchecked")
                    Converter<byte[], org.apache.pulsar.client.api.schema.GenericRecord, ?, byte[]> converter =
                        (Converter<byte[], org.apache.pulsar.client.api.schema.GenericRecord, ?, byte[]>) keyConverter;
                    byte[] keyBytes = converter.fromConnectData(genericRecord);
                    return Optional.of(new String(keyBytes, StandardCharsets.UTF_8));
                } catch (Exception e) {
                    throw new RuntimeException("Failed to serialize key", e);
                }
            } else {
                throw new IllegalStateException("Invalid key type: " +
                    (key != null ? key.getClass().getName() : "null") +
                    ". Expected byte[] or GenericRecord");
            }
        }

        @Override
        public Message<GenericRecord, MutationValue> getMutationMessage() {
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
