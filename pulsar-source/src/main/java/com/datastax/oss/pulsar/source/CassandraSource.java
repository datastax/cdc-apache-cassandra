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
import com.datastax.cassandra.cdc.pulsar.CDCSchema;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.vavr.Tuple3;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Source;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.apache.tinkerpop.gremlin.structure.T;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
    PulsarClient client = null;
    Consumer<KeyValue<MutationKey, MutationValue>> consumer = null;
    Converter converter;
    Cache<MutationKey, Set<String>> mutationCache = Caffeine.newBuilder()
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

        if (Strings.isNullOrEmpty(cassandraSourceConfig.getConverter())) {
            throw new IllegalArgumentException("Converter not set.");
        }
        this.converter = (Converter) Class.forName(cassandraSourceConfig.getConverter())
                .getDeclaredConstructor().newInstance();

        this.client = sourceContext.getPulsarClient();
        this.consumer = client.newConsumer(CDCSchema.kvSchema)
                .consumerName("CDC Consumer")
                .topic(cassandraSourceConfig.getDirtyTopicName())
                .autoUpdatePartitions(true)
                .subscriptionName(cassandraSourceConfig.getDirtySubscriptionName())
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscriptionMode(SubscriptionMode.Durable)
                .keySharedPolicy(KeySharedPolicy.autoSplitHashRange())
                .subscribe();
        log.debug("Starting source connector topic={} subscription={}",
                cassandraSourceConfig.getDirtyTopicName(),
                cassandraSourceConfig.getDirtySubscriptionName());
    }

    @Override
    public void close() throws Exception {
        this.cassandraClient.close();
    }

    public Set<String> getMutationCRCs(MutationKey mutationKey) {
        return mutationCache.getIfPresent(mutationKey);
    }

    public Set<String> addMutationMd5(MutationKey mutationKey, String md5Digest) {
        Set<String> crcs = getMutationCRCs(mutationKey);
        if(crcs == null)
            crcs = new HashSet<>();
        crcs.add(md5Digest);
        mutationCache.put(mutationKey, crcs);
        return crcs;
    }

    public boolean isMutationProcessed(MutationKey mutationKey, String md5Digest) {
        Set<String> digests = getMutationCRCs(mutationKey);
        return digests != null && digests.contains(md5Digest);
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
        final Consumer<KeyValue<MutationKey, MutationValue>> consumerFinal = consumer;
        log.debug("reading from topic={}", cassandraSourceConfig.getDirtyTopicName());
        while(true) {
            Message<KeyValue<MutationKey, MutationValue>> msg = consumer.receive();
            final KeyValue<MutationKey, MutationValue> kv = msg.getValue();
            final MutationKey mutationKey = kv.getKey();
            final MutationValue mutationValue = kv.getValue();

            log.debug("Message from producer={} msgId={} key={} value={}\n",
                    msg.getProducerName(), msg.getMessageId(), kv.getKey(), kv.getValue());

            final Message<KeyValue<MutationKey, MutationValue>> msgFinal = msg;
            if(isMutationProcessed(mutationKey, mutationValue.getMd5Digest()) == false) {
                try {
                    Tuple3<Row, ConsistencyLevel, KeyspaceMetadata> tuple =
                            cassandraClient.selectRow(mutationKey, kv.getValue().getNodeId(),
                                    Lists.newArrayList(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_ONE));
                    final Record record = converter.convert(mutationKey, tuple._1, tuple._3);
                    acknowledge(consumerFinal, msgFinal);
                    return record;
                } catch(Exception e) {
                    log.error("error", e);
                    negativeAcknowledge(consumerFinal, msgFinal);
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

    <T> java.util.function.Consumer<T> acknowledgeConsumer(final Consumer<KeyValue<MutationKey, MutationValue>> consumer,
                                                           final Message<KeyValue<MutationKey, MutationValue>> message) {
        return new java.util.function.Consumer<T>() {
            @Override
            public void accept(T t) {
                acknowledge(consumer, message);
            }
        };
    }

    // Acknowledge the message so that it can be deleted by the message broker
    void acknowledge(final Consumer<KeyValue<MutationKey, MutationValue>> consumer,
                     final Message<KeyValue<MutationKey, MutationValue>> message) {
        try {
            consumer.acknowledge(message);
        } catch(PulsarClientException e) {
            log.error("acknowledge error", e);
            consumer.negativeAcknowledge(message);
        }
    }

    void negativeAcknowledge(final Consumer<KeyValue<MutationKey, MutationValue>> consumer,
                             final Message<KeyValue<MutationKey, MutationValue>> message) {
        consumer.negativeAcknowledge(message);
    }
}