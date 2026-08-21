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
import com.datastax.oss.cdc.CqlLogicalTypes;
import com.datastax.oss.cdc.MutationCache;
import com.datastax.oss.cdc.MutationValue;
import com.datastax.oss.cdc.Version;
import com.datastax.oss.cdc.converters.NativeAvroRowConverter;
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
import com.datastax.oss.kafka.source.converters.NativeAvroConverter;
import com.datastax.oss.kafka.source.converters.NativeJsonConverter;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.vavr.Tuple2;
import io.vavr.Tuple3;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Conversions;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class KafkaCassandraSourceTask extends SourceTask implements SchemaChangeListener {

    CassandraSourceConnectorConfig config;
    volatile CassandraClient cassandraClient;
    KafkaConsumer<byte[], byte[]> consumer;
    String eventsTopic;
    String outputTopic;
    String heartbeatTopic;
    List<TopicPartition> assignedPartitions;

    NativeAvroConverter mutationKeyConverter;
    Converter<byte[], ?> keyConverter;

    Optional<Pattern> columnPattern = Optional.empty();
    MutationCache<String> mutationCache;

    volatile ConverterAndQuery valueConverterAndQuery;
    private Object emptyValue;

    List<ExecutorService> queryExecutors;

    final AtomicLong batchTotalLatency = new AtomicLong(0);
    final AtomicLong batchTotalQuery = new AtomicLong(0);
    long[] batchAvgLatencyList = new long[10];
    int batchAvgLatencyHead = 0;
    int batchAvgLatencyListSize = 0;
    long consecutiveUnavailableException = 0;

    private static final org.apache.avro.Schema MUTATION_VALUE_SCHEMA;

    static {
        org.apache.avro.Schema nullableString = org.apache.avro.Schema.createUnion(
                org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL),
                org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING));
        org.apache.avro.Schema nullableStringArray = org.apache.avro.Schema.createUnion(
                org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL),
                org.apache.avro.Schema.createArray(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING)));
        MUTATION_VALUE_SCHEMA = org.apache.avro.Schema.createRecord("MutationValue", null, "com.datastax.oss.cdc", false,
                Arrays.asList(
                        new org.apache.avro.Schema.Field("md5Digest", nullableString, null, (Object) null),
                        new org.apache.avro.Schema.Field("nodeId", nullableString, null, (Object) null),
                        new org.apache.avro.Schema.Field("columns", nullableStringArray, null, (Object) null)));
    }

    public KafkaCassandraSourceTask() {
        SpecificData.get().addLogicalTypeConversion(new CqlLogicalTypes.CqlVarintConversion());
        SpecificData.get().addLogicalTypeConversion(new CqlLogicalTypes.CqlDecimalConversion());
        SpecificData.get().addLogicalTypeConversion(new NativeAvroRowConverter.CqlDurationConversion());
        SpecificData.get().addLogicalTypeConversion(new Conversions.UUIDConversion());
    }

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = new CassandraSourceConnectorConfig(KafkaCassandraSourceConnector.remapConverterProps(props));
        if (!com.google.common.base.Strings.isNullOrEmpty(config.getColumnsRegexp()) && !".*".equals(config.getColumnsRegexp())) {
            this.columnPattern = Optional.of(Pattern.compile(config.getColumnsRegexp()));
        }
        this.eventsTopic = config.getEventsTopic();
        this.outputTopic = config.getOutputTopic();
        Preconditions.checkArgument(!com.google.common.base.Strings.isNullOrEmpty(outputTopic), "output.topic not set");
        this.heartbeatTopic = com.google.common.base.Strings.isNullOrEmpty(config.getHeartbeatTopic())
                ? outputTopic + "-heartbeat"
                : config.getHeartbeatTopic();

        Properties consumerProps = InternalConsumerProperties.build(config);
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, config.getInternalConsumerGroupId());
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        this.consumer = new KafkaConsumer<>(consumerProps);

        String partitionsCsv = props.get(KafkaCassandraSourceConnector.INTERNAL_CONSUMER_PARTITIONS_CONFIG);
        Preconditions.checkArgument(partitionsCsv != null && !partitionsCsv.isEmpty(), "internal.consumer.partitions not set");
        this.assignedPartitions = new ArrayList<>();
        for (String p : partitionsCsv.split(",")) {
            assignedPartitions.add(new TopicPartition(eventsTopic, Integer.parseInt(p.trim())));
        }
        consumer.assign(assignedPartitions);
        for (TopicPartition tp : assignedPartitions) {
            Map<String, Object> offset = context.offsetStorageReader().offset(sourcePartition(tp));
            if (offset != null && offset.get("offset") != null) {
                consumer.seek(tp, (Long) offset.get("offset"));
            } else {
                consumer.seekToBeginning(Collections.singleton(tp));
            }
        }

        this.mutationCache = new MutationCache<>(
                config.getCacheMaxDigests(),
                config.getCacheMaxCapacity(),
                Duration.ofMillis(config.getCacheExpireAfterMs()));
        log.info("Starting Kafka source task eventsTopic={} outputTopic={} heartbeatTopic={} partitions={} query.executors={}",
                eventsTopic, outputTopic, heartbeatTopic, assignedPartitions, config.getQueryExecutors());
    }

    private Map<String, Object> sourcePartition(TopicPartition tp) {
        Map<String, Object> map = new HashMap<>();
        map.put("topic", tp.topic());
        map.put("partition", tp.partition());
        return map;
    }

    private Map<String, Object> sourceOffset(long offset) {
        Map<String, Object> map = new HashMap<>();
        map.put("offset", offset);
        return map;
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
        this.cassandraClient = new CassandraClient(this.config, Version.getVersion(),
                Optional.ofNullable(config.getInstanceName()).orElse("kafka-cassandra-source-task"), this);
        Tuple2<KeyspaceMetadata, TableMetadata> tuple = cassandraClient.getTableMetadata(this.config.getKeyspaceName(), this.config.getTableName());
        Preconditions.checkArgument(tuple._1 != null, String.format(Locale.ROOT, "Keyspace %s does not exist", this.config.getKeyspaceName()));
        Preconditions.checkArgument(tuple._2 != null, String.format(Locale.ROOT, "Table %s.%s does not exist", this.config.getKeyspaceName(), this.config.getTableName()));
        this.keyConverter = createConverter(getKeyConverterClass(), tuple._1, tuple._2, tuple._2.getPrimaryKey());
        this.mutationKeyConverter = new NativeAvroConverter(tuple._1, tuple._2, tuple._2.getPrimaryKey());
        setValueConverterAndQuery(tuple._1, tuple._2);
    }

    private boolean isPrimaryKeyOnlyTable(TableMetadata tableMetadata) {
        return tableMetadata.getColumns().size() == tableMetadata.getPrimaryKey().size() &&
                new HashSet<>(tableMetadata.getPrimaryKey()).containsAll(tableMetadata.getColumns().values());
    }

    synchronized void setValueConverterAndQuery(KeyspaceMetadata ksm, TableMetadata tableMetadata) {
        try {
            boolean isPrimaryKeyOnlyTable = isPrimaryKeyOnlyTable(tableMetadata);
            List<ColumnMetadata> columns = tableMetadata.getColumns().values().stream()
                    .filter(c -> isPrimaryKeyOnlyTable || !tableMetadata.getPrimaryKey().contains(c))
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
        } catch (Exception e) {
            log.error("Unexpected error", e);
            throw new RuntimeException(e);
        }
    }

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
                ? this.config.isJsonOutputFormat() ? NativeJsonConverter.class : NativeAvroConverter.class
                : this.config.getKeyConverterClass();
    }

    Class<?> getValueConverterClass() {
        return this.config.getValueConverterClass() == null
                ? this.config.isJsonOutputFormat() ? NativeJsonConverter.class : NativeAvroConverter.class
                : this.config.getValueConverterClass();
    }

    @SuppressWarnings("unchecked")
    Converter<byte[], ?> createConverter(Class<?> converterClass, KeyspaceMetadata ksm, TableMetadata tableMetadata, List<ColumnMetadata> columns)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        return (Converter<byte[], ?>) converterClass
                .getDeclaredConstructor(KeyspaceMetadata.class, TableMetadata.class, List.class)
                .newInstance(ksm, tableMetadata, columns);
    }

    @Override
    public void stop() {
        close();
    }

    @Override
    public void close() {
        log.info("Stopping Kafka source task");
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
        if (this.consumer != null) {
            this.consumer.close();
        }
    }

    private void initQueryExecutors() {
        this.queryExecutors = new ArrayList<>(this.config.getQueryExecutors());
        for (int i = 0; i < this.config.getQueryExecutors(); i++) {
            this.queryExecutors.add(Executors.newSingleThreadExecutor());
        }
    }

    private <T> Future<T> executeOrdered(Object key, Callable<T> task) {
        Preconditions.checkArgument(key != null, "message key should not be null");
        Preconditions.checkState(queryExecutors != null, "queryExecutors should not be null");
        int threadIdx = Math.abs(Objects.hashCode(key)) % queryExecutors.size();
        return queryExecutors.get(threadIdx).submit(task);
    }

    private void adjustExecutors() {
        long batchAvgLatency = this.batchTotalLatency.get() / this.batchTotalQuery.get();
        this.batchAvgLatencyList[this.batchAvgLatencyHead] = batchAvgLatency;
        this.batchAvgLatencyHead = (this.batchAvgLatencyHead + 1) % this.batchAvgLatencyList.length;
        this.batchAvgLatencyListSize = Math.min(batchAvgLatencyListSize + 1, this.batchAvgLatencyList.length);

        long latencyTotal = 0;
        for (int i = 0; i < this.batchAvgLatencyListSize; i++) {
            latencyTotal += this.batchAvgLatencyList[i];
        }
        long mobileAvgLatency = latencyTotal / batchAvgLatencyListSize;
        if (mobileAvgLatency < config.getQueryMinMobileAvgLatency() && queryExecutors.size() < config.getQueryExecutors()) {
            queryExecutors.add(Executors.newSingleThreadExecutor());
            log.info("mobileAvgLatency={}, increasing the query executor to {} threads", mobileAvgLatency, queryExecutors.size());
        }
        if (mobileAvgLatency > config.getQueryMaxMobileAvgLatency() && queryExecutors.size() > 1) {
            queryExecutors.remove(queryExecutors.size() - 1).shutdown();
            log.info("mobileAvgLatency={}, decreasing the query executor to {} threads", mobileAvgLatency, queryExecutors.size());
        }
    }

    private void decreaseExecutors(Throwable throwable) {
        if (queryExecutors.size() > 1) {
            int numberOfThreadToRemove = Math.max(1, queryExecutors.size() / 10);
            for (int i = 0; i < numberOfThreadToRemove; i++) {
                queryExecutors.remove(queryExecutors.size() - 1).shutdown();
            }
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

    private GenericRecord decodeAvroRecord(byte[] bytes, org.apache.avro.Schema schema) throws IOException {
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        org.apache.avro.generic.GenericDatumReader<GenericRecord> reader = new org.apache.avro.generic.GenericDatumReader<>(schema);
        return reader.read(null, decoder);
    }

    private MutationValue decodeMutationValue(byte[] bytes) throws IOException {
        GenericRecord record = decodeAvroRecord(bytes, MUTATION_VALUE_SCHEMA);
        Object md5 = record.get("md5Digest");
        Object nodeId = record.get("nodeId");
        Object columnsValue = record.get("columns");
        String[] columns = null;
        if (columnsValue instanceof List) {
            columns = ((List<?>) columnsValue).stream().map(Object::toString).toArray(String[]::new);
        }
        return new MutationValue(
                md5 != null ? md5.toString() : null,
                nodeId != null ? UUID.fromString(nodeId.toString()) : null,
                columns);
    }

    private SourceRecord buildSourceRecord(ConsumerRecord<byte[], byte[]> rec, Object key, Object value) {
        TopicPartition tp = new TopicPartition(rec.topic(), rec.partition());
        return new SourceRecord(
                sourcePartition(tp),
                sourceOffset(rec.offset() + 1),
                outputTopic,
                null,
                org.apache.kafka.connect.data.Schema.BYTES_SCHEMA,
                key,
                org.apache.kafka.connect.data.Schema.BYTES_SCHEMA,
                value);
    }

    private SourceRecord buildHeartbeatRecord(ConsumerRecord<byte[], byte[]> rec) {
        TopicPartition tp = new TopicPartition(rec.topic(), rec.partition());
        return new SourceRecord(
                sourcePartition(tp),
                sourceOffset(rec.offset() + 1),
                heartbeatTopic,
                null,
                org.apache.kafka.connect.data.Schema.BYTES_SCHEMA,
                rec.key(),
                null,
                null);
    }

    private void seekBackToBatchStart(Map<TopicPartition, Long> firstOffsetInBatch) {
        for (Map.Entry<TopicPartition, Long> entry : firstOffsetInBatch.entrySet()) {
            consumer.seek(entry.getKey(), entry.getValue());
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<SourceRecord> poll() throws InterruptedException {
        batchTotalLatency.set(0);
        batchTotalQuery.set(0);
        if (this.queryExecutors == null) {
            initQueryExecutors();
        }
        try {
            maybeInitCassandraClient();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(1));
        if (records.isEmpty()) {
            return Collections.emptyList();
        }

        Map<TopicPartition, Long> firstOffsetInBatch = new HashMap<>();
        List<CompletableFuture<SourceRecord>> futures = new ArrayList<>();
        try {
            for (ConsumerRecord<byte[], byte[]> rec : records) {
                TopicPartition tp = new TopicPartition(rec.topic(), rec.partition());
                firstOffsetInBatch.putIfAbsent(tp, rec.offset());

                GenericRecord mutationKeyRecord;
                List<Object> pk;
                MutationValue mutationValue;
                try {
                    mutationKeyRecord = decodeAvroRecord(rec.key(), mutationKeyConverter.nativeSchema);
                    pk = mutationKeyConverter.fromConnectData(mutationKeyRecord);
                    mutationValue = decodeMutationValue(rec.value());
                } catch (IOException e) {
                    throw new RuntimeException("Cannot decode message at offset " + rec.offset(), e);
                }
                String cacheKey = Base64.getEncoder().encodeToString(rec.key());
                ConverterAndQuery converterAndQueryFinal = this.valueConverterAndQuery;

                CompletableFuture<SourceRecord> future = new CompletableFuture<>();
                executeOrdered(cacheKey, () -> {
                    try {
                        if (mutationCache.isMutationProcessed(cacheKey, mutationValue.getMd5Digest())) {
                            future.complete(buildHeartbeatRecord(rec));
                            return null;
                        }
                        List<Object> nonNullPkValues = pk.stream().filter(Objects::nonNull).collect(Collectors.toList());
                        long start = System.currentTimeMillis();
                        Tuple3<Row, ConsistencyLevel, UUID> tuple = cassandraClient.selectRow(
                                nonNullPkValues,
                                mutationValue.getNodeId(),
                                Lists.newArrayList(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_ONE),
                                getSelectStatement(converterAndQueryFinal, nonNullPkValues.size()),
                                mutationValue.getMd5Digest());
                        long end = System.currentTimeMillis();
                        batchTotalLatency.addAndGet(end - start);
                        batchTotalQuery.incrementAndGet();
                        Object value = tuple._1 == null ? this.emptyValue : converterAndQueryFinal.getConverter().toConnectData(tuple._1);
                        if (ConsistencyLevel.LOCAL_QUORUM.equals(tuple._2())
                                && (!config.getCacheOnlyIfCoordinatorMatch() || (tuple._3 != null && tuple._3.equals(mutationValue.getNodeId())))) {
                            mutationCache.addMutationMd5(cacheKey, mutationValue.getMd5Digest());
                        }
                        Object key = config.isAvroOutputFormat() ? rec.key() : keyConverter.fromConnectData(mutationKeyRecord);
                        future.complete(buildSourceRecord(rec, key, value));
                    } catch (Throwable err) {
                        future.completeExceptionally(err);
                    }
                    return null;
                });
                futures.add(future);
            }

            List<SourceRecord> sourceRecords = new ArrayList<>(futures.size());
            for (CompletableFuture<SourceRecord> future : futures) {
                SourceRecord sourceRecord = future.join();
                if (sourceRecord != null) {
                    sourceRecords.add(sourceRecord);
                }
            }
            if (batchTotalQuery.get() > 0) {
                adjustExecutors();
            }
            consecutiveUnavailableException = 0;
            return sourceRecords;
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof ExecutionException) {
                cause = cause.getCause();
            }
            log.info("CompletionException cause:", cause);
            if (cause instanceof com.datastax.oss.driver.api.core.servererrors.ReadTimeoutException
                    || cause instanceof com.datastax.oss.driver.api.core.servererrors.OverloadedException) {
                decreaseExecutors(cause);
            } else if (cause instanceof com.datastax.oss.driver.api.core.AllNodesFailedException) {
                // just retry
            } else {
                throw e;
            }
            seekBackToBatchStart(firstOffsetInBatch);
            backoffRetry(cause);
            return Collections.emptyList();
        } catch (com.datastax.oss.driver.api.core.AllNodesFailedException e) {
            log.info("AllNodesFailedException:", e);
            seekBackToBatchStart(firstOffsetInBatch);
            backoffRetry(e);
            return Collections.emptyList();
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
}
