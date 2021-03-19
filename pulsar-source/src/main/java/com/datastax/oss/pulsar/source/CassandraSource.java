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

import com.datastax.cassandra.cdc.MutationKey;
import com.datastax.cassandra.cdc.MutationValue;
import com.datastax.cassandra.cdc.converter.Converter;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.pulsar.source.converters.AvroConverter;
import com.datastax.oss.pulsar.source.converters.JsonConverter;
import com.datastax.oss.pulsar.source.converters.ProtobufConverter;
import com.datastax.oss.pulsar.source.converters.StringConverter;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.typesafe.config.ConfigUtil;
import io.vavr.Tuple3;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Source;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.apache.tinkerpop.gremlin.structure.T;

import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Cassandra source that treats incoming cassandra updates on the input mutation topic
 * and publish rows on the output topic.
 */
@Connector(
        name = "cassandra",
        type = IOType.SOURCE,
        help = "The CassandraSource is used for moving messages from Cassandra to Pulsar.",
        configClass = CassandraSourceConfig.class)
@Slf4j
public class CassandraSource implements Source<Object> {

    CassandraSourceConfig cassandraSourceConfig;
    CassandraClient cassandraClient;
    Consumer<KeyValue<Object, MutationValue>> consumer = null;

    String dirtyTopicName;
    Converter keyConverter;
    Converter valueConverter;

    Cache<String, Set<String>> mutationCache = Caffeine.newBuilder()
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .maximumSize(1000)
            .build();

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        cassandraSourceConfig = CassandraSourceConfig.load(config);
        if(Strings.isNullOrEmpty(cassandraSourceConfig.getContactPoints())) {
            throw new IllegalArgumentException("Cassandra contactPoints not set.");
        }
        this.cassandraClient = createClient(cassandraSourceConfig.getContactPoints());

        TableMetadata tableMetadata = cassandraClient.getTableMetadata(cassandraSourceConfig.getKeyspace(), cassandraSourceConfig.getTable());
        if (tableMetadata == null) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Table %s.%s does not exist.",
                    cassandraSourceConfig.getKeyspace(), cassandraSourceConfig.getTable()));
        }
        if(Strings.isNullOrEmpty(cassandraSourceConfig.getKeyConverter())) {
            throw new IllegalArgumentException("Key converter not set.");
        }
        this.keyConverter = (Converter) Class.forName(cassandraSourceConfig.getKeyConverter())
                .getDeclaredConstructor(TableMetadata.class, Set.class)
                .newInstance(tableMetadata, tableMetadata.getPrimaryKey().stream()
                        .map(c->c.getName().toString())
                        .collect(Collectors.toSet()));

        if(Strings.isNullOrEmpty(cassandraSourceConfig.getValueConverter())) {
            throw new IllegalArgumentException("Value converter not set.");
        }
        this.valueConverter = (Converter) Class.forName(cassandraSourceConfig.getValueConverter())
                .getDeclaredConstructor(TableMetadata.class, Set.class)
                .newInstance(tableMetadata, tableMetadata.getColumns().values().stream()
                        .filter(c -> !tableMetadata.getPrimaryKey().contains(c))
                        .map(c->c.getName().toString())
                        .collect(Collectors.toSet()));

        Schema<KeyValue<Object, MutationValue>> schema = Schema.KeyValue(
                Schema.AUTO_CONSUME(),
                JSONSchema.of(MutationValue.class),
                KeyValueEncodingType.SEPARATED);
        this.dirtyTopicName = cassandraSourceConfig.getDirtyTopicPrefix() + cassandraSourceConfig.getKeyspace() + "." + cassandraSourceConfig.getTable();
        this.consumer = sourceContext.newConsumerBuilder(schema)
                .consumerName("CDC Consumer")
                .topic(dirtyTopicName)
                .autoUpdatePartitions(true)
                .subscriptionName(cassandraSourceConfig.getDirtySubscriptionName())
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscriptionMode(SubscriptionMode.Durable)
                .keySharedPolicy(KeySharedPolicy.autoSplitHashRange())
                .subscribe();
        log.debug("Starting source connector topic={} subscription={}",
                dirtyTopicName,
                cassandraSourceConfig.getDirtySubscriptionName());
    }

    @Override
    public void close() throws Exception {
        this.cassandraClient.close();
    }

    public Set<String> getMutationCRCs(String mutationKey) {
        return mutationCache.getIfPresent(mutationKey);
    }

    public Set<String> addMutationMd5(String mutationKey, String md5Digest) {
        Set<String> crcs = getMutationCRCs(mutationKey);
        if(crcs == null)
            crcs = new HashSet<>();
        crcs.add(md5Digest);
        mutationCache.put(mutationKey, crcs);
        return crcs;
    }

    public boolean isMutationProcessed(String mutationKey, String md5Digest) {
        Set<String> digests = getMutationCRCs(mutationKey);
        return digests != null && digests.contains(md5Digest);
    }

    @SuppressWarnings("unchecked")
    Converter<?, Row, Object[]> getConverter(TableMetadata tableMetadata, Set<String> columns, String converterClassName)
            throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException,
            InvocationTargetException, InstantiationException {
        Class<? extends Converter<?, Row, Object[]>> converterClazz =
                (Class<? extends Converter<?, Row, Object[]>>) Class.forName(converterClassName);
        return converterClazz.getDeclaredConstructor(TableMetadata.class, Set.class).newInstance(tableMetadata, columns);
    }

    Converter<?, Row, Object[]> getConverter(TableMetadata tableMetadata, Set<String> columns, SchemaType schemaType) {
        switch(schemaType) {
            case AVRO:
                return new AvroConverter(tableMetadata, columns);
            case JSON:
                return new JsonConverter(tableMetadata, columns);
            case PROTOBUF:
                return new ProtobufConverter(tableMetadata, columns);
            case STRING:
                return new StringConverter(tableMetadata, columns);
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
    public Record<Object> read() throws Exception {
        final Consumer<KeyValue<Object, MutationValue>> consumerFinal = consumer;
        log.debug("reading from topic={}", dirtyTopicName);
        while(true) {
            Message<KeyValue<Object, MutationValue>> msg = consumer.receive();
            final KeyValue<Object, MutationValue> kv = msg.getValue();
            final Object mutationKey = kv.getKey();
            final MutationValue mutationValue = kv.getValue();

            log.debug("Message from producer={} msgId={} key={} value={}\n",
                    msg.getProducerName(), msg.getMessageId(), kv.getKey(), kv.getValue());

            final Message<KeyValue<Object, MutationValue>> msgFinal = msg;
            if(isMutationProcessed(msg.getKey(), mutationValue.getMd5Digest()) == false) {
                try {
                    Object[] pkColumns = (Object[]) keyConverter.fromConnectData(mutationKey);
                    Tuple3<Row, ConsistencyLevel, KeyspaceMetadata> tuple =
                            cassandraClient.selectRow(cassandraSourceConfig, pkColumns, kv.getValue().getNodeId(),
                                    Lists.newArrayList(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_ONE));

                    Object value = valueConverter.toConnectData(tuple._1);
                    KeyValue<Object, Object> keyValue = new KeyValue(mutationKey, value);
                    final Record record = new Record() {
                        @Override
                        public Schema getSchema() {
                            return Schema.KeyValue(keyConverter.getSchema(), valueConverter.getSchema(), KeyValueEncodingType.SEPARATED);
                        }

                        @Override
                        public Optional<String> getKey() {
                            String encodedKey = Base64.getEncoder().encodeToString(keyConverter.getSchema().encode(mutationKey));
                            return Optional.of(encodedKey);
                        }

                        @Override
                        public KeyValue getValue() {
                            return keyValue;
                        }
                    };
                    acknowledge(consumerFinal, msgFinal);
                    return record;
                } catch(Exception e) {
                    log.error("error", e);
                    negativeAcknowledge(consumerFinal, msgFinal);
                    throw e;
                }
            } else {
                acknowledge(consumerFinal, msgFinal);
            }
        }
    }

    private CassandraClient createClient(String roots) {
        String[] hosts = roots.split(",");
        if(hosts.length <= 0) {
            throw new RuntimeException("Invalid cassandra roots");
        }
        CqlSessionBuilder cqlSessionBuilder = CqlSession.builder()
                .withLocalDatacenter(cassandraSourceConfig.getLocalDc());
        for(int i = 0; i < hosts.length; ++i) {
            String[] hostPort = hosts[i].split(":");
            int port = hostPort.length > 1 ? Integer.valueOf(hostPort[1]) : 9042;
            InetSocketAddress endpoint = new InetSocketAddress(hostPort[0], port);
            cqlSessionBuilder.addContactPoint(endpoint);
        }
        return new CassandraClient(cqlSessionBuilder.build());
    }

    <T> java.util.function.Consumer<T> acknowledgeConsumer(final Consumer<KeyValue<Object, MutationValue>> consumer,
                                                           final Message<KeyValue<Object, MutationValue>> message) {
        return new java.util.function.Consumer<T>() {
            @Override
            public void accept(T t) {
                acknowledge(consumer, message);
            }
        };
    }

    // Acknowledge the message so that it can be deleted by the message broker
    void acknowledge(final Consumer<KeyValue<Object, MutationValue>> consumer,
                     final Message<KeyValue<Object, MutationValue>> message) {
        try {
            consumer.acknowledge(message);
        } catch(PulsarClientException e) {
            log.error("acknowledge error", e);
            consumer.negativeAcknowledge(message);
        }
    }

    void negativeAcknowledge(final Consumer<KeyValue<Object, MutationValue>> consumer,
                             final Message<KeyValue<Object, MutationValue>> message) {
        consumer.negativeAcknowledge(message);
    }
}