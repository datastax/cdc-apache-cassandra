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

import com.datastax.cassandra.cdc.MutationValue;
import com.datastax.oss.cdc.CassandraClient;
import com.datastax.oss.cdc.CassandraSourceConnectorConfig;
import com.datastax.oss.cdc.MutationCache;
import com.datastax.oss.cdc.Version;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.*;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.pulsar.source.converters.AvroConverter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.datastax.oss.cdc.ConfigUtil;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.vavr.Tuple2;
import io.vavr.Tuple3;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
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
import org.eclipse.jetty.util.BlockingArrayQueue;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
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

    ExecutorService[] queryExecutors;

    private final BlockingArrayQueue<MyKVRecord> buffer = new BlockingArrayQueue<>();

    private <T> Future<T> executeOrdered(Object key, Callable<T> task) {
        return queryExecutors[Math.abs(Objects.hashCode(key)) % queryExecutors.length].submit(task);
    }

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        try {
            Map<String, String> processorConfig = ConfigUtil.flatString(config);
            log.info("openCassandraSource {}", config);
            this.config = new CassandraSourceConnectorConfig(processorConfig);
            this.queryExecutors = new ExecutorService[this.config.getQueryExecutors()];
            for (int i = 0; i < queryExecutors.length; i++) {
                queryExecutors[i] = Executors.newSingleThreadExecutor();
            }
            this.cassandraClient = new CassandraClient(this.config, Version.getVersion(), sourceContext.getSourceName(), this);

            if (Strings.isNullOrEmpty(this.config.getEventsTopic())) {
                throw new IllegalArgumentException("Events topic not set.");
            }

            Tuple2<KeyspaceMetadata, TableMetadata> tuple =
                    cassandraClient.getTableMetadata(
                            this.config.getKeyspaceName(),
                            this.config.getTableName());
            if (tuple._2 == null) {
                throw new IllegalArgumentException(String.format(Locale.ROOT, "Table %s.%s does not exist.",
                        this.config.getKeyspaceName(),
                        this.config.getTableName()));
            }
            this.pkColumns = tuple._2.getPrimaryKey().stream().map(c -> c.getName().asInternal()).collect(Collectors.toList());
            if (this.config.getKeyConverterClass() == null) {
                throw new IllegalArgumentException("Key converter not defined.");
            }
            this.keyConverter = createConverter(this.config.getKeyConverterClass(), tuple._1, tuple._2, tuple._2.getPrimaryKey());
            this.mutationKeyConverter = new AvroConverter(tuple._1, tuple._2, tuple._2.getPrimaryKey());

            if (this.config.getValueConverterClass() == null) {
                throw new IllegalArgumentException("Value converter not defined.");
            }
            setValueConverterAndQuery(tuple._1, tuple._2);

            if (!Strings.isNullOrEmpty(this.config.getColumnsRegexp()) &&
                    !".*".equals(this.config.getColumnsRegexp())) {
                this.columnPattern = Optional.of(Pattern.compile(this.config.getColumnsRegexp()));
            }

            this.dirtyTopicName = this.config.getEventsTopic();
            ConsumerBuilder<KeyValue<GenericRecord, MutationValue>> consumerBuilder = sourceContext.newConsumerBuilder(eventsSchema)
                    .consumerName("CDC Consumer")
                    .topic(dirtyTopicName)
                    .subscriptionName(this.config.getEventsSubscriptionName())
                    .subscriptionType(SubscriptionType.Key_Shared)
                    .subscriptionMode(SubscriptionMode.Durable)
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .keySharedPolicy(KeySharedPolicy.autoSplitHashRange());
            this.consumer = consumerBuilder.subscribe();

            this.mutationCache = new MutationCache<>(
                    this.config.getCacheMaxDigests(),
                    this.config.getCacheMaxCapacity(),
                    Duration.ofMillis(this.config.getCacheExpireAfterMs()));

            log.debug("Starting source connector topic={} subscription={}",
                    dirtyTopicName,
                    this.config.getEventsSubscriptionName());
        } catch (Throwable err) {
            log.error("error on open", err);
            throw new RuntimeException(err);
        }
    }

    boolean supportedCqlTypes(ColumnMetadata cm) {
        switch (cm.getType().getProtocolCode()) {
            case ProtocolConstants.DataType.ASCII:
            case ProtocolConstants.DataType.VARCHAR:
            case ProtocolConstants.DataType.BOOLEAN:
            case ProtocolConstants.DataType.BLOB:
            case ProtocolConstants.DataType.DATE:
            case ProtocolConstants.DataType.TIME:
            case ProtocolConstants.DataType.TIMESTAMP:
            case ProtocolConstants.DataType.UUID:
            case ProtocolConstants.DataType.TIMEUUID:
            case ProtocolConstants.DataType.TINYINT:
            case ProtocolConstants.DataType.SMALLINT:
            case ProtocolConstants.DataType.INT:
            case ProtocolConstants.DataType.BIGINT:
            case ProtocolConstants.DataType.DOUBLE:
            case ProtocolConstants.DataType.FLOAT:
            case ProtocolConstants.DataType.INET:
            case ProtocolConstants.DataType.UDT:
                return true;
        }
        return false;
    }

    synchronized void setValueConverterAndQuery(KeyspaceMetadata ksm, TableMetadata tableMetadata) {
        try {
            List<ColumnMetadata> columns = tableMetadata.getColumns().values().stream()
                    .filter(c -> !tableMetadata.getPrimaryKey().contains(c))
                    .filter(c -> !columnPattern.isPresent() || columnPattern.get().matcher(c.getName().asInternal()).matches())
                    .filter(c -> supportedCqlTypes(c))
                    .collect(Collectors.toList());
            log.info("Schema update for table {}.{} replicated columns={}", ksm.getName(), tableMetadata.getName(),
                    columns.stream().map(c -> c.getName().asInternal()).collect(Collectors.toList()));
            this.valueConverterAndQuery = new ConverterAndQuery(
                    tableMetadata.getKeyspace().asInternal(),
                    tableMetadata.getName().asInternal(),
                    createConverter(config.getValueConverterClass(), ksm, tableMetadata, columns),
                    cassandraClient.buildProjectionClause(columns),
                    cassandraClient.buildPrimaryKeyClause(tableMetadata),
                    new ConcurrentHashMap<>());
            log.debug("valueConverterAndQuery={}", this.valueConverterAndQuery);
        } catch (Exception e) {
            log.error("Unexpected error", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Build the CQL prepared statement for the specified PK length.
     * NOTE: The prepared statement cannot be build from the schema listener thread to avoid a possible deadlock.
     * @param valueConverterAndQuery
     * @param pkLength
     * @return preparedStatement
     */
    synchronized PreparedStatement getSelectStatement(ConverterAndQuery valueConverterAndQuery, int pkLength) {
        return valueConverterAndQuery.getPreparedStatements().computeIfAbsent(pkLength, k ->
                cassandraClient.prepareSelect(
                        valueConverterAndQuery.keyspaceName,
                        valueConverterAndQuery.tableName,
                        valueConverterAndQuery.projectionClause,
                        valueConverterAndQuery.primaryKeyClause,
                        k));
    }

    Converter createConverter(Class<?> converterClass, KeyspaceMetadata ksm, TableMetadata tableMetadata, List<ColumnMetadata> columns)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        return (Converter) converterClass
                .getDeclaredConstructor(KeyspaceMetadata.class, TableMetadata.class, List.class)
                .newInstance(ksm, tableMetadata, columns);
    }

    @Override
    public void close() throws Exception {
        if (this.cassandraClient != null)
            this.cassandraClient.close();
        this.mutationCache = null;
        if (queryExecutors != null) {
            for (ExecutorService thread : queryExecutors) {
                thread.shutdownNow();
            }
        }
    }

    /**
     * Reads the next message from source.
     * If source does not have any new messages, this call should block.
     *
     * @return next message from source.  The return result should never be null
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    public Record<GenericRecord> read() throws Exception
    {
        log.debug("reading from topic={}", dirtyTopicName);
        MyKVRecord fromBuffer = buffer.poll();
        if (fromBuffer != null) {
            consumer.acknowledge(fromBuffer.msg);
            return (Record) fromBuffer;
        }
        // this methods returns only if the buffer holds at least one record
        ensureBuffer();
        fromBuffer = buffer.poll();
        consumer.acknowledge(fromBuffer.msg);
        return fromBuffer;
    }

    private void ensureBuffer() throws Exception {
        if (!buffer.isEmpty()) {
            throw new IllegalStateException("Buffer is not empty");
        }
        List<MyKVRecord> newRecords = fillBuffer();
        while (newRecords.isEmpty()) {
            newRecords = fillBuffer();
        }
        buffer.addAll(newRecords);
    }

    @SuppressWarnings("unchecked")
    private List<MyKVRecord> fillBuffer() throws Exception {
        List<MyKVRecord> newBuffer = new ArrayList<>();
        // we want to fill the buffer
        // this method will block until we receive at least one record
        while (newBuffer.size() <= this.config.getBatchSize()) {
            final Message<KeyValue<GenericRecord, MutationValue>> msg = consumer.receive(1, TimeUnit.SECONDS);
            if (msg == null) {
                if (!newBuffer.isEmpty()) {
                    log.debug("no message received, buffer size {}", newBuffer.size());
                    // no more records within the timeout, but we have at least one record
                    break;
                } else {
                    log.debug("no message received, buffer size {}", newBuffer.size());
                    continue;
                }
            }
            final KeyValue<GenericRecord, MutationValue> kv = msg.getValue();
            final GenericRecord mutationKey = kv.getKey();
            final MutationValue mutationValue = kv.getValue();

            log.info("Message from producer={} msgId={} key={} value={} schema {}\n",
                    msg.getProducerName(), msg.getMessageId(), kv.getKey(), kv.getValue(), msg.getReaderSchema().orElse(null));

            try {
                List<Object> pk = (List<Object>) mutationKeyConverter.fromConnectData(mutationKey);
                // ensure the schema is the one used when building the struct.
                final ConverterAndQuery converterAndQueryFinal = this.valueConverterAndQuery;

                CompletableFuture<KeyValue<Object, Object>> queryResult = new CompletableFuture<>();
                // we have to process sequentially the records from the same key
                // otherwise our mutation cache will not be enough efficient
                // in deduplicating mutations coming from different nodes
                executeOrdered(msg.getKey(), () -> {
                    try {
                        if (mutationCache.isMutationProcessed(msg.getKey(), mutationValue.getMd5Digest())) {
                            log.info("message key={} md5={} already processed", msg.getKey(), mutationValue.getMd5Digest());
                            // discard duplicated mutation
                            consumer.acknowledge(msg);
                            queryResult.complete(null);
                            return null;
                        }

                        List<Object> nonNullPkValues = pk.stream().filter(e -> e != null).collect(Collectors.toList());
                        Tuple3<Row, ConsistencyLevel, UUID> tuple = cassandraClient.selectRow(
                                nonNullPkValues,
                                mutationValue.getNodeId(),
                                Lists.newArrayList(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_ONE),
                                getSelectStatement(converterAndQueryFinal, nonNullPkValues.size()),
                                mutationValue.getMd5Digest());
                        Object value = tuple._1 == null ? null : converterAndQueryFinal.getConverter().toConnectData(tuple._1);
                        if (!config.getCacheOnlyIfCoordinatorMatch()
                                || (tuple._3 != null && tuple._3.equals(mutationValue.getNodeId()))) {
                            log.info("addMutation key={} md5={} pk={}", msg.getKey(), mutationValue.getMd5Digest(), nonNullPkValues);
                            // cache the mutation digest if the coordinator is the source of this event.
                            mutationCache.addMutationMd5(msg.getKey(), mutationValue.getMd5Digest());
                        }
                        queryResult.complete(new KeyValue(mutationKey, value));
                    } catch (Throwable err) {
                        queryResult.completeExceptionally(err);
                    }
                    return null;
                });
                final MyKVRecord record = new MyKVRecord(converterAndQueryFinal, queryResult, msg);
                newBuffer.add(record);
            } catch (Exception e) {
                log.error("error", e);
                // fail every message in the buffer
                for (MyKVRecord record : newBuffer) {
                    negativeAcknowledge(consumer, record.getMsg());
                }
                negativeAcknowledge(consumer, msg);
                throw e;
            }
        }
        if (newBuffer.isEmpty()) {
            throw new IllegalStateException("Buffer cannot be empty here");
        }

        List<MyKVRecord> useFullRecords = new ArrayList<>(newBuffer.size());
        try {
            int countDiscarded = 0;
            long start = System.currentTimeMillis();
            // wait for all queries to complete
            for (MyKVRecord record : newBuffer) {
                KeyValue res = record.keyValue.join();
                if (res != null) {
                    // if the result is "null" the mutation has been discarded
                    useFullRecords.add(record);
                } else {
                    countDiscarded++;
                }
            }
            long time = System.currentTimeMillis() - start;
            long speed = (long) (time * 1000.0 / newBuffer.size());
            log.info("Query time for {} msgs {} ms {} msgs/s discarded {} duplicate mutations", newBuffer.size(), time, speed, countDiscarded);
        } catch (Exception e) {
            log.error("error", e);
            // fail every message in the buffer
            for (MyKVRecord record : newBuffer) {
                negativeAcknowledge(consumer, record.getMsg());
            }
            throw e;
        }

        return useFullRecords;
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
        log.info("onTableUpdated {} {}", current, previous);
        if (current.getKeyspace().asInternal().equals(config.getKeyspaceName())
                && current.getName().asInternal().equals(config.getTableName())) {
            KeyspaceMetadata ksm = cassandraClient.getCqlSession().getMetadata().getKeyspace(current.getKeyspace()).get();
            setValueConverterAndQuery(ksm, current);
        }
    }

    @SneakyThrows
    @Override
    public void onUserDefinedTypeCreated(@NonNull UserDefinedType type) {
        log.info("onUserDefinedTypeCreated {} {}", type);
        if (type.getKeyspace().asInternal().equals(config.getKeyspaceName())) {
            KeyspaceMetadata ksm = cassandraClient.getCqlSession().getMetadata().getKeyspace(type.getKeyspace()).get();
            setValueConverterAndQuery(ksm, ksm.getTable(config.getTableName()).get());
        }
    }

    @Override
    public void onUserDefinedTypeDropped(@NonNull UserDefinedType type) {

    }

    @SneakyThrows
    @Override
    public void onUserDefinedTypeUpdated(@NonNull UserDefinedType userDefinedType, @NonNull UserDefinedType userDefinedType1) {
        log.info("onUserDefinedTypeUpdated {} {}", userDefinedType, userDefinedType1);
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
                return keyValue.toCompletableFuture().get();
            } catch (Exception err) {
                throw new RuntimeException(err);
            }
        }
    }
}
