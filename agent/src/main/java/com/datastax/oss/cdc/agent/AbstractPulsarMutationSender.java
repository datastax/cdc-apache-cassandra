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
package com.datastax.oss.cdc.agent;

import com.datastax.oss.cdc.CqlLogicalTypes;
import com.datastax.oss.cdc.MutationValue;
import com.datastax.oss.cdc.agent.exceptions.CassandraConnectorSchemaException;
import com.datastax.oss.cdc.NativeSchemaWrapper;
import com.datastax.oss.cdc.Murmur3MessageRouter;
import com.datastax.oss.cdc.Constants;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class AbstractPulsarMutationSender<T> implements MutationSender<T>, AutoCloseable {

    public static final String SCHEMA_DOC_PREFIX = "Primary key schema for table ";

    static {
        // register AVRO logical types conversion
        SpecificData.get().addLogicalTypeConversion(new CqlLogicalTypes.CqlVarintConversion());
        SpecificData.get().addLogicalTypeConversion(new CqlLogicalTypes.CqlDecimalConversion());
        SpecificData.get().addLogicalTypeConversion(new Conversions.UUIDConversion());
    }

    @AllArgsConstructor
    @ToString
    @EqualsAndHashCode
    public static class SchemaAndWriter {
        public final org.apache.avro.Schema schema;
        public final SpecificDatumWriter<GenericRecord> writer;
    }

    volatile PulsarClient client;
    final Map<String, Producer<KeyValue<byte[], MutationValue>>> producers = new ConcurrentHashMap<>();
    final Map<String, SchemaAndWriter> pkSchemas = new ConcurrentHashMap<>();

    final AgentConfig config;
    final boolean useMurmur3Partitioner;

    public AbstractPulsarMutationSender(AgentConfig config, boolean useMurmur3Partitioner) {
        this.config = config;
        this.useMurmur3Partitioner = useMurmur3Partitioner;
    }

    public abstract Schema getNativeSchema(String cql3Type);
    public abstract Object cqlToAvro(T t, String columnName, Object value);
    public abstract boolean isSupported(AbstractMutation<T> mutation);
    public abstract void incSkippedMutations();
    public abstract UUID getHostId();

    public SchemaAndWriter getPkSchema(String key) {
        return pkSchemas.get(key);
    }

    @Override
    public void initialize(AgentConfig config) throws PulsarClientException {
        try {
            ClientBuilder clientBuilder = PulsarClient.builder()
                    .serviceUrl(config.pulsarServiceUrl)
                    .memoryLimit(config.pulsarMemoryLimitBytes, SizeUnit.BYTES)
                    .enableTcpNoDelay(false);

            if (config.pulsarServiceUrl.startsWith("pulsar+ssl://")) {
                clientBuilder.tlsTrustStorePath(config.sslKeystorePath)
                        .tlsTrustStorePassword(config.sslTruststorePassword)
                        .tlsTrustStoreType(config.sslTruststoreType)
                        .tlsTrustCertsFilePath(config.tlsTrustCertsFilePath)
                        .useKeyStoreTls(config.useKeyStoreTls)
                        .allowTlsInsecureConnection(config.sslAllowInsecureConnection)
                        .enableTlsHostnameVerification(config.sslHostnameVerificationEnable);
                if (config.sslProvider != null) {
                    clientBuilder.sslProvider(config.sslProvider);
                }
                if (config.sslCipherSuites != null) {
                    clientBuilder.tlsCiphers(new HashSet<>(Arrays.asList(config.sslCipherSuites.split(","))));
                }
                if (config.sslEnabledProtocols != null) {
                    clientBuilder.tlsProtocols(new HashSet<>(Arrays.asList(config.sslEnabledProtocols.split(","))));
                }
            }
            if (config.pulsarAuthPluginClassName != null) {
                clientBuilder.authentication(config.pulsarAuthPluginClassName, config.pulsarAuthParams);
            }

            this.client = clientBuilder.build();
            log.info("Pulsar client connected");
        } catch (Exception e) {
            log.warn("Cannot connect to Pulsar:", e);
            throw e;
        }
    }

    public byte[] serializeAvroGenericRecord(org.apache.avro.generic.GenericRecord genericRecord, SpecificDatumWriter<org.apache.avro.generic.GenericRecord> datumWriter) {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            BinaryEncoder binaryEncoder = new EncoderFactory().binaryEncoder(byteArrayOutputStream, null);
            datumWriter.write(genericRecord, binaryEncoder);
            binaryEncoder.flush();
            return byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Build the AVRO schema for the primary key.
     * @param tableInfo
     * @return avroSchema of the table primary key
     */
    public SchemaAndWriter getAvroKeySchema(final TableInfo tableInfo) {
        return pkSchemas.computeIfAbsent(tableInfo.key(), k -> {
            List<Schema.Field> fields = new ArrayList<>();
            for (ColumnInfo cm : tableInfo.primaryKeyColumns()) {
                org.apache.avro.Schema.Field field = new org.apache.avro.Schema.Field(cm.name(), getNativeSchema(cm.cql3Type()));
                if (cm.isClusteringKey()) {
                    // clustering keys are optional
                    field = new org.apache.avro.Schema.Field(cm.name(), org.apache.avro.SchemaBuilder.unionOf().nullType().and().type(field.schema()).endUnion());
                }
                fields.add(field);
            }
            org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord(tableInfo.key(), SCHEMA_DOC_PREFIX + tableInfo.key(), tableInfo.name(), false, fields);
            return new SchemaAndWriter(avroSchema, new SpecificDatumWriter<>(avroSchema));
        });
    }

    @AllArgsConstructor
    @ToString
    public static class TopicAndProducerName {
        public final String topicName;
        public final String producerName;
    }

    public TopicAndProducerName topicAndProducerName(final TableInfo tm) {
        return new TopicAndProducerName(
                config.topicPrefix + tm.key(),
                "cdc-producer-" + getHostId() + "-" + tm.key());
    }

    /**
     * Build the Pulsar producer for the provided table metadata.
     * @param tm table metadata
     * @return the pulsar producer
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public Producer<KeyValue<byte[], MutationValue>> getProducer(final TableInfo tm) throws PulsarClientException {
        if (this.client == null) {
            synchronized (this) {
                if (this.client == null)
                    initialize(config);
            }
        }
        final TopicAndProducerName topicAndProducerName = topicAndProducerName(tm);
        return producers.computeIfAbsent(topicAndProducerName.topicName, k -> {
            try {
                org.apache.pulsar.client.api.Schema<KeyValue<byte[], MutationValue>> keyValueSchema = org.apache.pulsar.client.api.Schema.KeyValue(
                        new NativeSchemaWrapper(getAvroKeySchema(tm).schema, SchemaType.AVRO),
                        org.apache.pulsar.client.api.Schema.AVRO(MutationValue.class),
                        KeyValueEncodingType.SEPARATED);
                ProducerBuilder<KeyValue<byte[], MutationValue>> producerBuilder = client.newProducer(keyValueSchema)
                        .producerName(topicAndProducerName.producerName)
                        .topic(k)
                        .sendTimeout(0, TimeUnit.SECONDS)
                        .hashingScheme(HashingScheme.Murmur3_32Hash)
                        .blockIfQueueFull(true)
                        .maxPendingMessages(config.pulsarMaxPendingMessages)
                        .autoUpdatePartitions(true);

                if (config.pulsarBatchDelayInMs > 0) {
                    producerBuilder.enableBatching(true)
                            .batchingMaxPublishDelay(config.pulsarBatchDelayInMs, TimeUnit.MILLISECONDS);
                } else {
                    producerBuilder.enableBatching(false);
                }
                if (config.pulsarKeyBasedBatcher) {
                    // only for single non-partitioned topic and Key_Shared subscription source connector
                    producerBuilder.batcherBuilder(BatcherBuilder.KEY_BASED);
                }
                if (useMurmur3Partitioner) {
                    producerBuilder.messageRoutingMode(MessageRoutingMode.CustomPartition)
                            .messageRouter(Murmur3MessageRouter.instance);
                }
                log.info("Pulsar producer name={} created with batching delay={}ms",
                        topicAndProducerName.producerName, config.pulsarBatchDelayInMs);
                return producerBuilder.create();
            } catch (Exception e) {
                log.error("Failed to get a pulsar producer", e);
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * @param keySchema
     * @param mutation
     * @return The primary key as an AVRO GenericRecord
     */
    public org.apache.avro.generic.GenericRecord buildAvroKey(org.apache.avro.Schema keySchema, AbstractMutation<T> mutation) {
        org.apache.avro.generic.GenericRecord genericRecord = new org.apache.avro.generic.GenericData.Record(keySchema);
        int i = 0;
        for (ColumnInfo columnInfo : mutation.primaryKeyColumns()) {
            if (keySchema.getField(columnInfo.name()) == null)
                throw new CassandraConnectorSchemaException("Not a valid schema field: " + columnInfo.name());
            genericRecord.put(columnInfo.name(), cqlToAvro(mutation.getMetadata(), columnInfo.name(), mutation.getPkValues()[i++]));
        }
        return genericRecord;
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public CompletableFuture<MessageId> sendMutationAsync(final AbstractMutation<T> mutation) {
        if (!isSupported(mutation)) {
            incSkippedMutations();
            return CompletableFuture.completedFuture(null);
        }
        try {
            Producer<KeyValue<byte[], MutationValue>> producer = getProducer(mutation);
            SchemaAndWriter schemaAndWriter = getAvroKeySchema(mutation);
            TypedMessageBuilder<KeyValue<byte[], MutationValue>> messageBuilder = producer.newMessage();
            messageBuilder = messageBuilder
                    .value(new KeyValue(
                            serializeAvroGenericRecord(buildAvroKey(schemaAndWriter.schema, mutation), schemaAndWriter.writer),
                            mutation.mutationValue()))
                    .property(Constants.SEGMENT_AND_POSITION, mutation.getSegment() + ":" + mutation.getPosition())
                    .property(Constants.TOKEN, mutation.getToken().toString());
            // a WRITETIME property is only used by the connector to emit e2e latency metric, skip if the mutation is not timestamped
            if (mutation.getTs() != -1) {
                messageBuilder = messageBuilder.property(Constants.WRITETIME, mutation.getTs() + "");
            }
            return messageBuilder.sendAsync();
        } catch(Exception e) {
            CompletableFuture future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        }
    }

    /**
     * Closes this resource, relinquishing any underlying resources.
     * This method is invoked automatically on objects managed by the
     * {@code try}-with-resources statement.
     *
     * <p>While this interface method is declared to throw {@code
     * Exception}, implementers are <em>strongly</em> encouraged to
     * declare concrete implementations of the {@code close} method to
     * throw more specific exceptions, or to throw no exception at all
     * if the close operation cannot fail.
     *
     * <p> Cases where the close operation may fail require careful
     * attention by implementers. It is strongly advised to relinquish
     * the underlying resources and to internally <em>mark</em> the
     * resource as closed, prior to throwing the exception. The {@code
     * close} method is unlikely to be invoked more than once and so
     * this ensures that the resources are released in a timely manner.
     * Furthermore it reduces problems that could arise when the resource
     * wraps, or is wrapped, by another resource.
     *
     * <p><em>Implementers of this interface are also strongly advised
     * to not have the {@code close} method throw {@link
     * InterruptedException}.</em>
     * <p>
     * This exception interacts with a thread's interrupted status,
     * and runtime misbehavior is likely to occur if an {@code
     * InterruptedException} is {@linkplain Throwable#addSuppressed
     * suppressed}.
     * <p>
     * More generally, if it would cause problems for an
     * exception to be suppressed, the {@code AutoCloseable.close}
     * method should not throw it.
     *
     * <p>Note that unlike the {@link Closeable#close close}
     * method of {@link Closeable}, this {@code close} method
     * is <em>not</em> required to be idempotent.  In other words,
     * calling this {@code close} method more than once may have some
     * visible side effect, unlike {@code Closeable.close} which is
     * required to have no effect if called more than once.
     * <p>
     * However, implementers of this interface are strongly encouraged
     * to make their {@code close} methods idempotent.
     *
     * @throws Exception if this resource cannot be closed
     */
    @Override
    public void close() {
        try {
            if (client != null) {
                synchronized (this) {
                    if (client != null)
                        this.client.close();
                }
            }
        } catch (PulsarClientException e) {
            log.warn("close failed:", e);
        }
    }
}
