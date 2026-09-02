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
import com.datastax.oss.cdc.SourceUtil;
import com.datastax.oss.cdc.CassandraSourceConnectorConfig;
import com.datastax.oss.cdc.ConverterAndQuery;
import com.datastax.oss.cdc.CqlLogicalTypes;
import com.datastax.oss.cdc.MutationCache;
import com.datastax.oss.cdc.MutationValue;
import com.datastax.oss.cdc.SourceSchemaChangeListener;
import com.datastax.oss.cdc.Version;
import com.datastax.oss.cdc.converters.AvroRowConverter;
import com.datastax.oss.cdc.converters.ConverterFactory;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.kafka.source.converters.Converter;
import com.datastax.oss.kafka.source.converters.KafkaAvroConverter;
import com.datastax.oss.kafka.source.converters.KafkaJsonConverter;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.vavr.Tuple2;
import io.vavr.Tuple3;
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

import org.apache.bookkeeper.common.util.OrderedExecutor;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class KafkaCassandraSourceTask extends SourceTask implements SourceSchemaChangeListener {

    CassandraSourceConnectorConfig config;
    CassandraClient cassandraClient;
    KafkaConsumer<byte[], byte[]> consumer;
    String eventsTopic;
    String outputTopic;
    String heartbeatTopic;
    List<TopicPartition> assignedPartitions;

    KafkaAvroConverter mutationKeyConverter;
    Converter<byte[], ?> keyConverter;

    Optional<Pattern> columnPattern = Optional.empty();
    MutationCache<String> mutationCache;

    volatile ConverterAndQuery<Converter<byte[], ?>> valueConverterAndQuery;
    private Object emptyValue;

    OrderedExecutor queryExecutor;
    private long consecutiveUnavailableException = 0;

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
        SpecificData.get().addLogicalTypeConversion(new AvroRowConverter.CqlDurationConversion());
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
        log.info("Starting Kafka source task eventsTopic={} outputTopic={} heartbeatTopic={} partitions={} query.executors={} maxTasksInQueue={}",
                eventsTopic, outputTopic, heartbeatTopic, assignedPartitions,
                config.getQueryExecutors(), config.getQueryMaxTasksInQueue());
        this.queryExecutor = OrderedExecutor.newBuilder()
                .name("cdc-query-executor")
                .numThreads(config.getQueryExecutors())
                .maxTasksInQueue(config.getQueryMaxTasksInQueue())
                .build();
        initCassandraClientWithRetry();
    }

    private static final long INIT_RETRY_MAX_SINGLE_WAIT_MS = 5000L;

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
                consecutiveFailures = SourceUtil.backoffRetry(err, consecutiveFailures, this.config,
                        INIT_RETRY_MAX_SINGLE_WAIT_MS);
            }
        }
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

    void initCassandraClient() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, InstantiationException {
        this.cassandraClient = new CassandraClient(this.config, Version.getVersion(),
                Optional.ofNullable(config.getInstanceName()).orElse("kafka-cassandra-source-task"), this);
        Tuple2<KeyspaceMetadata, TableMetadata> tuple = cassandraClient.getTableMetadata(this.config.getKeyspaceName(), this.config.getTableName());
        Preconditions.checkArgument(tuple._1 != null, String.format(Locale.ROOT, "Keyspace %s does not exist", this.config.getKeyspaceName()));
        Preconditions.checkArgument(tuple._2 != null, String.format(Locale.ROOT, "Table %s.%s does not exist", this.config.getKeyspaceName(), this.config.getTableName()));
        this.keyConverter = createConverter(getKeyConverterClass(), tuple._1, tuple._2, tuple._2.getPrimaryKey());
        this.mutationKeyConverter = new KafkaAvroConverter(tuple._1, tuple._2, tuple._2.getPrimaryKey());
        setValueConverterAndQuery(tuple._1, tuple._2);
    }

    // TODO: schema evolution. This swaps the value converter/schema in place as soon as a
    // Cassandra table alteration is observed (see onTableUpdated below), with no compatibility
    // check (backward/forward/full) and no versioned publish to a schema registry. A downstream
    // consumer reading the data topic with the previous Avro schema can break as soon as this
    // runs, with no warning. Needs a registry (Confluent Schema Registry or Apicurio Registry,
    // see the design doc's schema registry discussion) before this can be made safe.
    public synchronized void setValueConverterAndQuery(KeyspaceMetadata ksm, TableMetadata tableMetadata) {
        try {
            this.valueConverterAndQuery = ConverterAndQuery.forTable(
                    config, columnPattern, cassandraClient, ksm, tableMetadata, getValueConverterClass(), log);
            this.emptyValue = config.isJsonOnlyOutputFormat() ? "{}".getBytes(StandardCharsets.UTF_8) : null;
        } catch (Exception e) {
            log.error("Unexpected error", e);
            throw new RuntimeException(e);
        }
    }

    synchronized PreparedStatement getSelectStatement(ConverterAndQuery<Converter<byte[], ?>> valueConverterAndQuery, int whereClauseLength) {
        return valueConverterAndQuery.prepareSelectStatement(cassandraClient, whereClauseLength);
    }

    Class<?> getKeyConverterClass() {
        return ConverterFactory.resolveConverterClass(
                config.getKeyConverterClass(), config.isJsonOutputFormat(), KafkaJsonConverter.class, KafkaAvroConverter.class);
    }

    Class<?> getValueConverterClass() {
        return ConverterFactory.resolveConverterClass(
                config.getValueConverterClass(), config.isJsonOutputFormat(), KafkaJsonConverter.class, KafkaAvroConverter.class);
    }

    Converter<byte[], ?> createConverter(Class<?> converterClass, KeyspaceMetadata ksm, TableMetadata tableMetadata, List<ColumnMetadata> columns)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        return ConverterFactory.create(converterClass, ksm, tableMetadata, columns);
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
        if (queryExecutor != null) {
            queryExecutor.shutdown();
        }
        if (this.consumer != null) {
            this.consumer.close();
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

    // TODO: schema evolution / schema registry support. Key and value are published as raw
    // Avro/JSON bytes under Schema.BYTES_SCHEMA, bypassing Kafka Connect's converter framework
    // entirely (see AbstractRowConverter). No schema ID is embedded and no registry (Confluent
    // Schema Registry or Apicurio Registry) is involved, so downstream consumers must know the
    // wire format out of band, and there is no compatibility check when the Cassandra table
    // schema changes (see setValueConverterAndQuery/onTableUpdated below).
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

    @Override
    @SuppressWarnings("unchecked")
    public List<SourceRecord> poll() throws InterruptedException {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(1));
        if (records.isEmpty()) {
            return Collections.emptyList();
        }

        while (true) {
            List<CompletableFuture<SourceRecord>> futures = new ArrayList<>();
            try {
                for (ConsumerRecord<byte[], byte[]> rec : records) {
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
                    ConverterAndQuery<Converter<byte[], ?>> converterAndQueryFinal = this.valueConverterAndQuery;

                    CompletableFuture<SourceRecord> future = new CompletableFuture<>();
                    queryExecutor.executeOrdered(cacheKey, () -> {
                        try {
                            if (mutationCache.isMutationProcessed(cacheKey, mutationValue.getMd5Digest())) {
                                future.complete(buildHeartbeatRecord(rec));
                                return;
                            }
                            List<Object> nonNullPkValues = pk.stream().filter(Objects::nonNull).collect(Collectors.toList());
                            Tuple3<Row, ConsistencyLevel, UUID> tuple = cassandraClient.selectRow(
                                    nonNullPkValues,
                                    mutationValue.getNodeId(),
                                    Lists.newArrayList(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_ONE),
                                    getSelectStatement(converterAndQueryFinal, nonNullPkValues.size()),
                                    mutationValue.getMd5Digest());
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
                consecutiveUnavailableException = 0;
                return sourceRecords;
            } catch (Exception e) {
                Throwable cause = e instanceof CompletionException && e.getCause() != null ? e.getCause() : e;
                if (cause instanceof ExecutionException && cause.getCause() != null) cause = cause.getCause();
                log.warn("Error processing batch, will retry:", cause);
                consecutiveUnavailableException = SourceUtil.backoffRetry(cause, consecutiveUnavailableException, config);
            }
        }
    }

    @Override
    public String getKeyspaceName() { return config.getKeyspaceName(); }

    @Override
    public String getTableName() { return config.getTableName(); }

    @Override
    public CassandraClient getCassandraClient() { return cassandraClient; }
}
