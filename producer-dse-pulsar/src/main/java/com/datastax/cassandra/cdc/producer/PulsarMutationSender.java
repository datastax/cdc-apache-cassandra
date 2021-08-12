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

import com.datastax.cassandra.cdc.MutationValue;
import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.schema.*;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Slf4j
public class PulsarMutationSender implements MutationSender<TableMetadata>, AutoCloseable {

    public static final String SCHEMA_DOC_PREFIX = "Primary key schema for table ";

    final ProducerConfig config;
    PulsarClient client;
    final Map<String, Producer<KeyValue<GenericRecord, MutationValue>>> producers = new ConcurrentHashMap<>();
    final Map<String, Schema<GenericRecord>> schemas = new HashMap<>();
    final ImmutableMap<String, SchemaType> schemaTypes;

    public PulsarMutationSender(ProducerConfig config) {
        this.config = config;
        // Map Cassandra native types to Pulsar schema types
        schemaTypes = ImmutableMap.<String, SchemaType>builder()
                .put(UTF8Type.instance.asCQL3Type().toString(), SchemaType.STRING)
                .put(AsciiType.instance.asCQL3Type().toString(), SchemaType.STRING)
                .put(BooleanType.instance.asCQL3Type().toString(), SchemaType.BOOLEAN)
                .put(BytesType.instance.asCQL3Type().toString(), SchemaType.BYTES)
                .put(ByteType.instance.asCQL3Type().toString(), SchemaType.INT32)   // INT8 not supported by AVRO
                .put(ShortType.instance.asCQL3Type().toString(), SchemaType.INT32)  // INT16 not supported by AVRO
                .put(Int32Type.instance.asCQL3Type().toString(), SchemaType.INT32)
                .put(IntegerType.instance.asCQL3Type().toString(), SchemaType.INT64)
                .put(LongType.instance.asCQL3Type().toString(), SchemaType.INT64)

                .put(FloatType.instance.asCQL3Type().toString(), SchemaType.FLOAT)
                .put(DoubleType.instance.asCQL3Type().toString(), SchemaType.DOUBLE)

                .put(InetAddressType.instance.asCQL3Type().toString(), SchemaType.INT64)

                .put(TimestampType.instance.asCQL3Type().toString(), SchemaType.TIMESTAMP)
                .put(SimpleDateType.instance.asCQL3Type().toString(), SchemaType.DATE)
                .put(TimeType.instance.asCQL3Type().toString(), SchemaType.TIME)
                //schemas.put(DurationType.instance.asCQL3Type().toString(), NanoDuration.builder().optional());

                // convert UUID to String
                .put(UUIDType.instance.asCQL3Type().toString(), SchemaType.STRING)
                .put(TimeUUIDType.instance.asCQL3Type().toString(), SchemaType.STRING)
                .build();
    }

    @SuppressWarnings({"rawtypes","unchecked"})
    public Schema getKeySchema(final TableMetadata tm) {
        final String key = tm.keyspace + "." + tm.name;
        return schemas.computeIfAbsent(key, k -> {
            List<ColumnMetadata> primaryKeyColumns = new ArrayList<>();
            tm.primaryKeyColumns().forEach(primaryKeyColumns::add);
            RecordSchemaBuilder schemaBuilder = SchemaBuilder.record(k).doc(SCHEMA_DOC_PREFIX + k);
            int i = 0;
            for (ColumnMetadata cm : primaryKeyColumns) {
                schemaBuilder
                        .field(cm.name.toString())
                        .type(schemaTypes.get(primaryKeyColumns.get(i++).type.asCQL3Type().toString()));
            }
            SchemaInfo schemaInfo = schemaBuilder.build(SchemaType.AVRO);
            return Schema.generic(schemaInfo);
        });
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public Producer<KeyValue<GenericRecord, MutationValue>> getProducer(final TableMetadata tm) {
        final String topicName = config.topicPrefix + tm.keyspace + "." + tm.name;
        final String producerName = "pulsar-producer-" + StorageService.instance.getLocalHostId() + "-" + topicName;
        return producers.compute(topicName, (k, v) -> {
            if (v == null) {
                try {
                    Schema<KeyValue<GenericRecord, MutationValue>> keyValueSchema = Schema.KeyValue(
                            getKeySchema(tm),
                            Schema.AVRO(MutationValue.class),
                            KeyValueEncodingType.SEPARATED);
                    Producer<KeyValue<GenericRecord, MutationValue>> producer = client.newProducer(keyValueSchema)
                            .producerName(producerName)
                            .topic(k)
                            .sendTimeout(15, TimeUnit.SECONDS)
                            .hashingScheme(HashingScheme.Murmur3_32Hash)
                            .blockIfQueueFull(true)
                            .enableBatching(true)
                            .batchingMaxPublishDelay(1, TimeUnit.MILLISECONDS)
                            .batcherBuilder(BatcherBuilder.KEY_BASED)
                            .create();
                    log.info("Pulsar producer name={} created", producerName);
                    return producer;
                } catch (Exception e) {
                    log.error("Failed to get a pulsar producer", e);
                    throw new RuntimeException(e);
                }
            }
            return v;
        });
    }

    @Override
    public void initialize(ProducerConfig config) throws PulsarClientException {
        try {
            ClientBuilder clientBuilder = PulsarClient.builder().serviceUrl(config.pulsarServiceUrl);

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
                    clientBuilder.tlsCiphers(new HashSet<String>(Arrays.asList(config.sslCipherSuites.split(","))));
                }
                if (config.sslEnabledProtocols != null) {
                    clientBuilder.tlsProtocols(new HashSet<String>(Arrays.asList(config.sslEnabledProtocols.split(","))));
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

    @SuppressWarnings("rawtypes")
    GenericRecord buildKey(Schema keySchema, List<CellData> primaryKey) {
        GenericRecordBuilder genericRecordBuilder = ((GenericSchema) keySchema).newRecordBuilder();
        for (CellData cell : primaryKey) {
            genericRecordBuilder.set(cell.name, cell.value);
        }
        return genericRecordBuilder.build();
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public CompletionStage<MessageId> sendMutationAsync(final Mutation<TableMetadata> mutation) throws PulsarClientException {
        if (this.client == null) {
            initialize(config);
        }
        Producer<KeyValue<GenericRecord, MutationValue>> producer = getProducer(mutation.getMetadata());
        Schema keySchema = getKeySchema(mutation.getMetadata());
        TypedMessageBuilder<KeyValue<GenericRecord, MutationValue>> messageBuilder = producer.newMessage();
        return messageBuilder
                .value(new KeyValue<GenericRecord, MutationValue>(
                        buildKey(keySchema, mutation.primaryKeyCells()),
                        mutation.mutationValue()))
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
