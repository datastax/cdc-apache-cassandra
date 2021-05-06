/**
 * Copyright DataStax, Inc 2021.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import com.datastax.oss.pulsar.source.converters.AvroConverter;
import com.datastax.oss.sink.pulsar.ConfigUtil;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.vavr.Tuple2;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.schema.AvroSchema;
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
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Cassandra source that treats incoming cassandra updates on the input events topic
 * and publish rows on the data topic.
 */
@Connector(
        name = "cassandra",
        type = IOType.SOURCE,
        help = "The CassandraSource is used for moving data from Cassandra to Pulsar.",
        configClass = CassandraSourceConfig.class)
@Slf4j
public class CassandraSource implements Source<GenericRecord>, SchemaChangeListener {

    CassandraSourceConnectorConfig cassandraSourceConnectorConfig;
    CassandraClient cassandraClient;
    Consumer<KeyValue<GenericRecord, MutationValue>> consumer = null;

    String dirtyTopicName;
    Converter mutationKeyConverter;
    Converter keyConverter;
    List<String> pkColumns;

    volatile ConverterAndQuery valueConverterAndQuery;   // modified on schema change
    volatile PreparedStatement selectStatement;
    volatile int selectHash = -1;

    Optional<Pattern> columnPattern = Optional.empty();

    MutationCache<String> mutationCache;

    Schema<KeyValue<GenericRecord, MutationValue>> eventsSchema = Schema.KeyValue(
            Schema.AUTO_CONSUME(),
            AvroSchema.of(MutationValue.class),
            KeyValueEncodingType.SEPARATED);

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        Map<String, String> processorConfig = ConfigUtil.flatString(config);
        this.cassandraSourceConnectorConfig = new CassandraSourceConnectorConfig(processorConfig);
        this.cassandraClient = new CassandraClient(cassandraSourceConnectorConfig, Version.getVersion(), sourceContext.getSourceName(), this);

        if (Strings.isNullOrEmpty(cassandraSourceConnectorConfig.getEventsTopic())) {
            throw new IllegalArgumentException("Events topic not set.");
        }

        Tuple2<KeyspaceMetadata, TableMetadata> tuple =
                cassandraClient.getTableMetadata(
                        cassandraSourceConnectorConfig.getKeyspaceName(),
                        cassandraSourceConnectorConfig.getTableName());
        if (tuple._2 == null) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Table %s.%s does not exist.",
                    cassandraSourceConnectorConfig.getKeyspaceName(),
                    cassandraSourceConnectorConfig.getTableName()));
        }
        this.pkColumns = tuple._2.getPrimaryKey().stream().map(c -> c.getName().asInternal()).collect(Collectors.toList());
        if (cassandraSourceConnectorConfig.getKeyConverterClass() == null) {
            throw new IllegalArgumentException("Key converter not defined.");
        }
        this.keyConverter = createConverter(cassandraSourceConnectorConfig.getKeyConverterClass(),
                tuple._1,
                tuple._2,
                tuple._2.getPrimaryKey());
        this.mutationKeyConverter = new AvroConverter(
                tuple._1,
                tuple._2,
                tuple._2.getPrimaryKey());

        if (cassandraSourceConnectorConfig.getValueConverterClass() == null) {
            throw new IllegalArgumentException("Value converter not defined.");
        }
        setValueConverterAndQuery(tuple._1, tuple._2);

        if (!Strings.isNullOrEmpty(cassandraSourceConnectorConfig.getColumnsRegexp()) &&
                !".*".equals(cassandraSourceConnectorConfig.getColumnsRegexp())) {
            this.columnPattern = Optional.of(Pattern.compile(cassandraSourceConnectorConfig.getColumnsRegexp()));
        }

        this.dirtyTopicName = cassandraSourceConnectorConfig.getEventsTopic();
        ConsumerBuilder<KeyValue<GenericRecord, MutationValue>> consumerBuilder = sourceContext.newConsumerBuilder(eventsSchema)
                .consumerName("CDC Consumer")
                .topic(dirtyTopicName)
                .subscriptionName(cassandraSourceConnectorConfig.getEventsSubscriptionName())
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscriptionMode(SubscriptionMode.Durable)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .keySharedPolicy(KeySharedPolicy.autoSplitHashRange());
        this.consumer = consumerBuilder.subscribe();
        this.mutationCache = new MutationCache<>(
                cassandraSourceConnectorConfig.getCacheMaxDigests(),
                cassandraSourceConnectorConfig.getCacheMaxDigests(),
                Duration.ofMillis(cassandraSourceConnectorConfig.getCacheExpireAfterMs()));
        log.debug("Starting source connector topic={} subscription={}",
                dirtyTopicName,
                cassandraSourceConnectorConfig.getEventsSubscriptionName());
    }

    synchronized void setValueConverterAndQuery(KeyspaceMetadata ksm, TableMetadata tableMetadata)
            throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        List<ColumnMetadata> columns = tableMetadata.getColumns().values().stream()
                .filter(c -> !tableMetadata.getPrimaryKey().contains(c))
                .filter(c -> !columnPattern.isPresent() || columnPattern.get().matcher(c.getName().asInternal()).matches())
                .collect(Collectors.toList());
        log.info("Schema update for table {}.{} replicated columns={}", ksm.getName(), tableMetadata.getName(),
                columns.stream().map(c -> c.getName().asInternal()).collect(Collectors.toList()));
        this.valueConverterAndQuery = new ConverterAndQuery(
                createConverter(cassandraSourceConnectorConfig.getValueConverterClass(), ksm, tableMetadata, columns),
                cassandraClient.buildSelect(tableMetadata, columns));
        // Invalidate the prepare statement if the query has changed.
        // We cannot build the statement here form a C* driver thread (can cause dead lock)
        if (valueConverterAndQuery.getQuery().hashCode() != this.selectHash) {
            this.selectStatement = null;
            this.selectHash = valueConverterAndQuery.getQuery().hashCode();
        }
    }

    // Build the prepared statement if needed
    synchronized PreparedStatement getSelectStatement() {
        if (this.selectStatement == null) {
            this.selectStatement = cassandraClient.prepareSelect(this.valueConverterAndQuery.getQuery());
        }
        return this.selectStatement;
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
    }

    /**
     * Reads the next message from source.
     * If source does not have any new messages, this call should block.
     *
     * @return next message from source.  The return result should never be null
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    public Record<GenericRecord> read() throws Exception {
        log.debug("reading from topic={}", dirtyTopicName);
        while (true) {
            final Message<KeyValue<GenericRecord, MutationValue>> msg = consumer.receive();
            final KeyValue<GenericRecord, MutationValue> kv = msg.getValue();
            final GenericRecord mutationKey = kv.getKey();
            final MutationValue mutationValue = kv.getValue();

            log.debug("Message from producer={} msgId={} key={} value={}\n",
                    msg.getProducerName(), msg.getMessageId(), kv.getKey(), kv.getValue());

            if (mutationCache.isMutationProcessed(msg.getKey(), mutationValue.getMd5Digest()) == false) {
                try {
                    List<Object> pk = (List<Object>) mutationKeyConverter.fromConnectData(mutationKey);
                    // ensure the schema is the one used when building the struct.
                    final ConverterAndQuery converterAndQueryFinal = this.valueConverterAndQuery;

                    Tuple2<Row, ConsistencyLevel> tuple = cassandraClient.selectRow(
                            pk,
                            mutationValue.getNodeId(),
                            Lists.newArrayList(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_ONE),
                            getSelectStatement());

                    Object value = tuple._1 == null ? null : converterAndQueryFinal.getConverter().toConnectData(tuple._1);
                    KeyValue<Object, Object> keyValue = new KeyValue(mutationKey, value);
                    final KVRecord record = new KVRecord() {
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
                            return keyValue;
                        }
                    };
                    acknowledge(consumer, msg);
                    mutationCache.addMutationMd5(msg.getKey(), mutationValue.getMd5Digest());
                    return record;
                } catch (Exception e) {
                    log.error("error", e);
                    negativeAcknowledge(consumer, msg);
                    throw e;
                }
            } else {
                acknowledge(consumer, msg);
            }
        }
    }

    <T> java.util.function.Consumer<T> acknowledgeConsumer(final Consumer<KeyValue<GenericRecord, MutationValue>> consumer,
                                                           final Message<KeyValue<GenericRecord, MutationValue>> message) {
        return new java.util.function.Consumer<T>() {
            @Override
            public void accept(T t) {
                acknowledge(consumer, message);
            }
        };
    }

    // Acknowledge the message so that it can be deleted by the message broker
    void acknowledge(final Consumer<KeyValue<GenericRecord, MutationValue>> consumer,
                     final Message<KeyValue<GenericRecord, MutationValue>> message) {
        try {
            consumer.acknowledge(message);
        } catch (PulsarClientException e) {
            log.error("acknowledge error", e);
            consumer.negativeAcknowledge(message);
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
        if (current.getKeyspace().asInternal().equals(cassandraSourceConnectorConfig.getKeyspaceName())
                && current.getName().asInternal().equals(cassandraSourceConnectorConfig.getTableName())) {
            KeyspaceMetadata ksm = cassandraClient.getCqlSession().getMetadata().getKeyspace(current.getKeyspace()).get();
            setValueConverterAndQuery(ksm, current);
        }
    }

    @SneakyThrows
    @Override
    public void onUserDefinedTypeCreated(@NonNull UserDefinedType type) {
        if (type.getKeyspace().asInternal().equals(cassandraSourceConnectorConfig.getKeyspaceName())) {
            KeyspaceMetadata ksm = cassandraClient.getCqlSession().getMetadata().getKeyspace(type.getKeyspace()).get();
            setValueConverterAndQuery(ksm, ksm.getTable(cassandraSourceConnectorConfig.getTableName()).get());
        }
    }

    @Override
    public void onUserDefinedTypeDropped(@NonNull UserDefinedType type) {

    }

    @SneakyThrows
    @Override
    public void onUserDefinedTypeUpdated(@NonNull UserDefinedType userDefinedType, @NonNull UserDefinedType userDefinedType1) {
        if (userDefinedType.getKeyspace().asCql(true).equals(cassandraSourceConnectorConfig.getKeyspaceName())) {
            KeyspaceMetadata ksm = cassandraClient.getCqlSession().getMetadata().getKeyspace(userDefinedType.getKeyspace()).get();
            setValueConverterAndQuery(ksm, ksm.getTable(cassandraSourceConnectorConfig.getTableName()).get());
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
