/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.datastax.oss.pulsar.source;

import com.datastax.oss.cdc.CassandraClient;
import com.datastax.oss.cdc.CassandraSourceConnectorConfig;
import com.datastax.oss.cdc.MutationCache;
import com.datastax.cassandra.cdc.MutationValue;
import com.datastax.oss.cdc.Version;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.*;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.pulsar.source.converters.AvroConverter;
import com.datastax.oss.pulsar.source.converters.JsonConverter;
import com.datastax.oss.pulsar.source.converters.ProtobufConverter;
import com.datastax.oss.pulsar.source.converters.StringConverter;
import com.datastax.oss.sink.pulsar.ConfigUtil;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.vavr.Tuple2;
import io.vavr.Tuple3;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.KVRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Source;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Cassandra source that treats incoming cassandra updates on the input events topic
 * and publish rows on the data topic.
 */
@Connector(
        name = "cassandra",
        type = IOType.SOURCE,
        help = "The CassandraSource is used for moving messages from Cassandra to Pulsar.",
        configClass = CassandraSourceConfig.class)
@Slf4j
public class CassandraSource implements Source<GenericRecord>, SchemaChangeListener {

    CassandraSourceConnectorConfig cassandraSourceConnectorConfig;
    CassandraClient cassandraClient;
    Consumer<KeyValue<GenericRecord, MutationValue>> consumer = null;

    String dirtyTopicName;
    Converter mutationKeyConverter;
    Converter keyConverter;
    Converter valueConverter;
    List<String> pkColumns;

    MutationCache<String> mutationCache;

    Schema<KeyValue<GenericRecord, MutationValue>> dirtySchema = Schema.KeyValue(
            Schema.AUTO_CONSUME(),
            AvroSchema.of(MutationValue.class),
            KeyValueEncodingType.SEPARATED);

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        Map<String, String> processorConfig = ConfigUtil.flatString(config);
        this.cassandraSourceConnectorConfig = new CassandraSourceConnectorConfig(processorConfig);
        this.cassandraClient = new CassandraClient(cassandraSourceConnectorConfig, Version.getVersion(), sourceContext.getSourceName(), this);

        if (Strings.isNullOrEmpty(cassandraSourceConnectorConfig.getEventsTopic())) {
            throw new IllegalArgumentException("Events topic prefix not set.");
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
        pkColumns = tuple._2.getPrimaryKey().stream().map(c -> c.getName().asCql(true)).collect(Collectors.toList());
        if(cassandraSourceConnectorConfig.getKeyConverterClass() == null) {
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

        if(cassandraSourceConnectorConfig.getValueConverterClass() == null) {
            throw new IllegalArgumentException("Value converter not defined.");
        }
        setValueConverter(tuple._1, tuple._2);


        this.dirtyTopicName = cassandraSourceConnectorConfig.getEventsTopic();
        ConsumerBuilder<KeyValue<GenericRecord, MutationValue>> consumerBuilder = sourceContext.newConsumerBuilder(dirtySchema)
                .consumerName("CDC Consumer")
                .topic(dirtyTopicName)
                .autoUpdatePartitions(true)
                .subscriptionName(cassandraSourceConnectorConfig.getEventsSubscriptionName())
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscriptionMode(SubscriptionMode.Durable)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .keySharedPolicy(KeySharedPolicy.autoSplitHashRange());
        this.consumer = consumerBuilder.subscribe();
        this.mutationCache = new MutationCache<>(3, 1024, Duration.ofHours(1));
        log.debug("Starting source connector topic={} subscription={}",
                dirtyTopicName,
                cassandraSourceConnectorConfig.getEventsSubscriptionName());
    }

    void setValueConverter(KeyspaceMetadata ksm, TableMetadata tableMetadata)
            throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        this.valueConverter = createConverter(cassandraSourceConnectorConfig.getValueConverterClass(),
                ksm,
                tableMetadata,
                tableMetadata.getColumns().values().stream()
                        .filter(c -> !tableMetadata.getPrimaryKey().contains(c))
                        .collect(Collectors.toList()));
    }

    Converter createConverter(Class<?> converterClass, KeyspaceMetadata ksm, TableMetadata tableMetadata, List<ColumnMetadata> columns)
            throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
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

    @SuppressWarnings("unchecked")
    Converter<?, Row, Object[]> getConverter(KeyspaceMetadata ksm, List<ColumnMetadata> columns, String converterClassName)
            throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException,
            InvocationTargetException, InstantiationException {
        Class<? extends Converter<?, Row, Object[]>> converterClazz =
                (Class<? extends Converter<?, Row, Object[]>>) Class.forName(converterClassName);
        return converterClazz.getDeclaredConstructor(KeyspaceMetadata.class, TableMetadata.class, List.class).newInstance(ksm, columns);
    }

    Converter<?, Row, Map<String, Object>> getConverter(KeyspaceMetadata ksm, TableMetadata tm, List<ColumnMetadata> columns, SchemaType schemaType) {
        switch(schemaType) {
            case AVRO:
                return new AvroConverter(ksm, tm, columns);
            case JSON:
                return new JsonConverter(ksm, tm, columns);
            case PROTOBUF:
                return new ProtobufConverter(ksm, tm, columns);
            case STRING:
                return new StringConverter(ksm, tm, columns);
            default:
                throw new UnsupportedOperationException();
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
    public Record<GenericRecord> read() throws Exception {
        log.debug("reading from topic={}", dirtyTopicName);
        while(true) {
            final Message<KeyValue<GenericRecord, MutationValue>> msg = consumer.receive();
            final KeyValue<GenericRecord, MutationValue> kv = msg.getValue();
            final Object mutationKey = kv.getKey();
            final MutationValue mutationValue = kv.getValue();

            log.debug("Message from producer={} msgId={} key={} value={}\n",
                    msg.getProducerName(), msg.getMessageId(), kv.getKey(), kv.getValue());

            if (mutationCache.isMutationProcessed(msg.getKey(), mutationValue.getMd5Digest()) == false) {
                try {
                    Map<String, Object> pk =  (Map<String, Object>) mutationKeyConverter.fromConnectData(mutationKey);
                    Tuple3<Row, ConsistencyLevel, KeyspaceMetadata> tuple =
                            cassandraClient.selectRow(
                                    cassandraSourceConnectorConfig.getKeyspaceName(),
                                    cassandraSourceConnectorConfig.getTableName(),
                                    pk,
                                    mutationValue.getNodeId(),
                                    Lists.newArrayList(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_ONE));

                    Object value = tuple._1 == null ? null : valueConverter.toConnectData(tuple._1);
                    KeyValue<Object, Object> keyValue = new KeyValue(mutationKey, value);
                    final Record record = new KVRecord() {
                        @Override
                        public Schema getKeySchema() {
                            return keyConverter.getSchema();
                        }

                        @Override
                        public Schema getValueSchema() {
                            return valueConverter.getSchema();
                        }

                        @Override
                        public KeyValueEncodingType getKeyValueEncodingType() {
                            return KeyValueEncodingType.SEPARATED;
                        }

                        @Override
                        public Optional<String> getKey() {
                            String encodedKey = Base64.getEncoder().encodeToString(mutationKeyConverter.getSchema().encode(mutationKey));
                            return Optional.of(encodedKey);
                        }

                        @Override
                        public KeyValue getValue() {
                            return keyValue;
                        }
                    };
                    acknowledge(consumer, msg);
                    mutationCache.addMutationMd5(msg.getKey(), mutationValue.getMd5Digest());
                    return record;
                } catch(Exception e) {
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
        } catch(PulsarClientException e) {
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
        if (current.getKeyspace().asCql(true).equals(cassandraSourceConnectorConfig.getKeyspaceName())
            && current.getName().asCql(true).equals(cassandraSourceConnectorConfig.getTableName())) {
            KeyspaceMetadata ksm = cassandraClient.getCqlSession().getMetadata().getKeyspace(cassandraSourceConnectorConfig.getKeyspaceName()).get();
            setValueConverter(ksm, current);
        }
    }

    @SneakyThrows
    @Override
    public void onUserDefinedTypeCreated(@NonNull UserDefinedType type) {
        if (type.getKeyspace().asCql(true).equals(cassandraSourceConnectorConfig.getKeyspaceName())) {
            KeyspaceMetadata ksm = cassandraClient.getCqlSession().getMetadata().getKeyspace(cassandraSourceConnectorConfig.getKeyspaceName()).get();
            setValueConverter(ksm, ksm.getTable(cassandraSourceConnectorConfig.getTableName()).get());
        }
    }

    @Override
    public void onUserDefinedTypeDropped(@NonNull UserDefinedType type) {

    }

    @Override
    public void onUserDefinedTypeUpdated(@NonNull UserDefinedType current, @NonNull UserDefinedType previous) {

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
