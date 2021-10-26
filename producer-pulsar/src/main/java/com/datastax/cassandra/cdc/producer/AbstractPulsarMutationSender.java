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
package com.datastax.cassandra.cdc.producer;

import com.datastax.cassandra.cdc.CqlLogicalTypes;
import com.datastax.cassandra.cdc.MutationValue;
import com.datastax.cassandra.cdc.producer.exceptions.CassandraConnectorSchemaException;
import com.datastax.pulsar.utils.AvroSchemaWrapper;
import com.datastax.pulsar.utils.Constants;
import com.datastax.pulsar.utils.Murmur3MessageRouter;
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
    static class AvroSchema {
        org.apache.avro.Schema schema;
        SpecificDatumWriter<GenericRecord> writer;
    }

    volatile PulsarClient client;
    final Map<String, Producer<KeyValue<byte[], MutationValue>>> producers = new ConcurrentHashMap<>();
    final Map<String, AvroSchema> avroSchemas = new ConcurrentHashMap<>();

    final ProducerConfig config;
    final String hostId;
    final boolean useMurmur3Partitioner;

    public AbstractPulsarMutationSender(ProducerConfig config, String hostId, boolean useMurmur3Partitioner) {
        this.config = config;
        this.hostId = hostId;
        this.useMurmur3Partitioner = useMurmur3Partitioner;
    }

    public abstract Schema getAvroSchema(String cql3Type);
    public abstract Object cqlToAvro(T t, String columnName, Object value);
    public abstract boolean isSupported(final T t);
    public abstract void incSkippedMutations();

    @Override
    public void initialize(ProducerConfig config) throws PulsarClientException {
        try {
            ClientBuilder clientBuilder = PulsarClient.builder()
                    .serviceUrl(config.pulsarServiceUrl)
                    .enableTcpNoDelay(false);

            if (config.pulsarServiceUrl.startsWith("pulsar+ssl://")) {
                clientBuilder.tlsTrustStorePath(config.sslKeystorePath)
                        .tlsTrustStorePassword(config.sslTruststorePassword)
                        .tlsTrustStoreType(config.sslTruststoreType)
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

    byte[] serializeAvroGenericRecord(org.apache.avro.generic.GenericRecord genericRecord, SpecificDatumWriter<org.apache.avro.generic.GenericRecord> datumWriter) {
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
    public AvroSchema getAvroKeySchema(final TableInfo tableInfo) {
        final String keyspaceAndTable = tableInfo.keyspace() + "." + tableInfo.name();
        return avroSchemas.computeIfAbsent(keyspaceAndTable, k -> {
            List<Schema.Field> fields = new ArrayList<>();
            for (ColumnInfo cm : tableInfo.primaryKeyColumns()) {
                org.apache.avro.Schema.Field field = new org.apache.avro.Schema.Field(cm.name(), getAvroSchema(cm.cql3Type()));
                if (cm.isClusteringKey()) {
                    // clustering keys are optional
                    field = new org.apache.avro.Schema.Field(cm.name(), org.apache.avro.SchemaBuilder.unionOf().nullType().and().type(field.schema()).endUnion());
                }
                fields.add(field);
            }
            org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord(keyspaceAndTable, SCHEMA_DOC_PREFIX + keyspaceAndTable, tableInfo.name(), false, fields);
            return new AvroSchema(avroSchema, new SpecificDatumWriter<>(avroSchema));
        });
    }

    /**
     * Build the Pulsar producer for the provided table metadata.
     * @param tm table metadata
     * @return the pulsar producer
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public Producer<KeyValue<byte[], MutationValue>> getProducer(final TableInfo tm) {
        final String topicName = config.topicPrefix + tm.keyspace() + "." + tm.name();
        final String producerName = "pulsar-producer-" + hostId + "-" + topicName;
        return producers.computeIfAbsent(topicName, k -> {
            try {
                org.apache.pulsar.client.api.Schema<KeyValue<byte[], MutationValue>> keyValueSchema = org.apache.pulsar.client.api.Schema.KeyValue(
                        new AvroSchemaWrapper(getAvroKeySchema(tm).schema),
                        org.apache.pulsar.client.api.Schema.AVRO(MutationValue.class),
                        KeyValueEncodingType.SEPARATED);
                ProducerBuilder<KeyValue<byte[], MutationValue>> producerBuilder = client.newProducer(keyValueSchema)
                        .producerName(producerName)
                        .topic(k)
                        .sendTimeout(0, TimeUnit.SECONDS)
                        .hashingScheme(HashingScheme.Murmur3_32Hash)
                        .blockIfQueueFull(true);
                if (config.pulsarBatchDelayInMs > 0) {
                    producerBuilder.enableBatching(true)
                            .batchingMaxPublishDelay(config.pulsarBatchDelayInMs, TimeUnit.MILLISECONDS)
                            .batcherBuilder(BatcherBuilder.KEY_BASED);
                } else {
                    producerBuilder.enableBatching(false);
                }
                if (useMurmur3Partitioner) {
                    producerBuilder.messageRoutingMode(MessageRoutingMode.CustomPartition)
                            .messageRouter(Murmur3MessageRouter.instance);
                }
                producerBuilder.autoUpdatePartitions(true);
                log.info("Pulsar producer name={} created with batching delay={}ms", producerName, config.pulsarBatchDelayInMs);
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
    org.apache.avro.generic.GenericRecord buildAvroKey(org.apache.avro.Schema keySchema, AbstractMutation<T> mutation) {
        org.apache.avro.generic.GenericRecord genericRecord = new org.apache.avro.generic.GenericData.Record(keySchema);
        for (CellData cell : mutation.primaryKeyCells()) {
            if (keySchema.getField(cell.name) == null)
                throw new CassandraConnectorSchemaException("Not a valid schema field: " + cell.name);
            genericRecord.put(cell.name, cqlToAvro(mutation.getMetadata(), cell.name, cell.value));
        }
        return genericRecord;
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public CompletableFuture<MessageId> sendMutationAsync(final AbstractMutation<T> mutation) throws PulsarClientException {
        if (!isSupported(mutation.getMetadata())) {
            incSkippedMutations();
            return CompletableFuture.completedFuture(null);
        }
        if (this.client == null) {
            initialize(config);
        }
        Producer<KeyValue<byte[], MutationValue>> producer = getProducer(mutation);
        AvroSchema avroSchema = getAvroKeySchema(mutation);
        TypedMessageBuilder<KeyValue<byte[], MutationValue>> messageBuilder = producer.newMessage();
        return messageBuilder
                .value(new KeyValue(
                        serializeAvroGenericRecord(buildAvroKey(avroSchema.schema, mutation), avroSchema.writer),
                        mutation.mutationValue()))
                .property(Constants.WRITETIME, mutation.getTs() + "")
                .property(Constants.SEGMENT_AND_POSITION, mutation.getSegment()  + ":" + mutation.getPosition())
                .property(Constants.TOKEN, mutation.getToken().toString())
                .sendAsync();
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
            this.client.close();
        } catch (PulsarClientException e) {
            log.warn("close failed:", e);
        }
    }
}
