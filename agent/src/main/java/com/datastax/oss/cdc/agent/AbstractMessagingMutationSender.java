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
import com.datastax.oss.cdc.Constants;
import com.datastax.oss.cdc.Murmur3MessageRouter;
import com.datastax.oss.cdc.NativeSchemaWrapper;
import com.datastax.oss.cdc.messaging.MessagingClient;
import com.datastax.oss.cdc.messaging.MessagingException;
import com.datastax.oss.cdc.messaging.MessageId;
import com.datastax.oss.cdc.messaging.MessageProducer;
import com.datastax.oss.cdc.messaging.config.*;
import com.datastax.oss.cdc.messaging.config.impl.*;
import com.datastax.oss.cdc.messaging.factory.MessagingClientFactory;
import com.datastax.oss.cdc.messaging.schema.SchemaDefinition;
import com.datastax.oss.cdc.messaging.schema.SchemaType;
import com.datastax.oss.cdc.messaging.schema.impl.BaseSchemaDefinition;
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Abstract base class for mutation senders using messaging abstraction layer.
 * Replaces AbstractPulsarMutationSender with provider-agnostic implementation.
 *
 * @param <T> Column metadata type (version-specific)
 */
@Slf4j
public abstract class AbstractMessagingMutationSender<T> implements MutationSender<T>, AutoCloseable {

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

    protected volatile MessagingClient messagingClient;
    protected final Map<String, MessageProducer<byte[], MutationValue>> producers = new ConcurrentHashMap<>();
    protected final Map<String, SchemaAndWriter> pkSchemas = new ConcurrentHashMap<>();

    protected final AgentConfig config;
    protected final boolean useMurmur3Partitioner;

    public AbstractMessagingMutationSender(AgentConfig config, boolean useMurmur3Partitioner) {
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
    public void initialize(AgentConfig config) throws MessagingException {
        try {
            // Build client configuration from AgentConfig
            ClientConfig clientConfig = buildClientConfig(config);
            
            // Create messaging client using factory
            this.messagingClient = MessagingClientFactory.create(clientConfig);
            
            MessagingProvider provider = determineProvider(config);
            String serviceUrl = provider == MessagingProvider.KAFKA ?
                config.kafkaBootstrapServers : config.pulsarServiceUrl;
            log.info("Messaging client ({}) connected to {}", provider, serviceUrl);
        } catch (Exception e) {
            log.warn("Cannot connect to messaging system:", e);
            throw new MessagingException("Failed to initialize messaging client", e);
        }
    }

    /**
     * Build client configuration from agent config.
     */
    protected ClientConfig buildClientConfig(AgentConfig config) {
        // Determine provider from config
        MessagingProvider provider = determineProvider(config);
        
        ClientConfigBuilder builder = ClientConfigBuilder.builder()
                .provider(provider);

        if (provider == MessagingProvider.PULSAR) {
            builder.serviceUrl(config.pulsarServiceUrl)
                   .memoryLimitBytes(config.pulsarMemoryLimitBytes);

            // Add SSL configuration if needed
            if (config.pulsarServiceUrl != null && config.pulsarServiceUrl.startsWith("pulsar+ssl://")) {
                builder.sslConfig(buildSslConfig(config));
            }

            // Add authentication if configured
            if (config.pulsarAuthPluginClassName != null) {
                builder.authConfig(buildAuthConfig(config));
            }
        } else if (provider == MessagingProvider.KAFKA) {
            builder.serviceUrl(config.kafkaBootstrapServers);

            // Add SSL configuration if needed (Kafka uses different URL scheme)
            if (config.sslKeystorePath != null || config.tlsTrustCertsFilePath != null) {
                builder.sslConfig(buildSslConfig(config));
            }

            // Add Kafka-specific provider properties
            Map<String, Object> providerProps = new HashMap<>();
            if (config.kafkaAcks != null) {
                providerProps.put("acks", config.kafkaAcks);
            }
            if (config.kafkaCompressionType != null) {
                providerProps.put("compression.type", config.kafkaCompressionType);
            }
            if (config.kafkaBatchSize > 0) {
                providerProps.put("batch.size", config.kafkaBatchSize);
            }
            if (config.kafkaLingerMs >= 0) {
                providerProps.put("linger.ms", config.kafkaLingerMs);
            }
            if (config.kafkaMaxInFlightRequests > 0) {
                providerProps.put("max.in.flight.requests.per.connection", config.kafkaMaxInFlightRequests);
            }
            if (config.kafkaSchemaRegistryUrl != null) {
                providerProps.put("schema.registry.url", config.kafkaSchemaRegistryUrl);
            }
            builder.providerProperties(providerProps);
        }

        return builder.build();
    }

    /**
     * Determine messaging provider from config.
     */
    protected MessagingProvider determineProvider(AgentConfig config) {
        if (config.messagingProvider != null) {
            String provider = config.messagingProvider.toUpperCase();
            if ("KAFKA".equals(provider)) {
                return MessagingProvider.KAFKA;
            } else if ("PULSAR".equals(provider)) {
                return MessagingProvider.PULSAR;
            }
        }
        // Default to PULSAR for backward compatibility
        return MessagingProvider.PULSAR;
    }

    /**
     * Build SSL configuration from agent config.
     */
    protected SslConfig buildSslConfig(AgentConfig config) {
        SslConfigBuilder builder = SslConfigBuilder.builder()
                .trustedCertificates(config.tlsTrustCertsFilePath)
                .hostnameVerificationEnabled(config.sslHostnameVerificationEnable);

        if (config.useKeyStoreTls) {
            builder.keyStorePath(config.sslKeystorePath)
                   .keyStorePassword(config.sslTruststorePassword)
                   .keyStoreType(config.sslTruststoreType)
                   .trustStorePath(config.sslKeystorePath)
                   .trustStorePassword(config.sslTruststorePassword)
                   .trustStoreType(config.sslTruststoreType);
        }

        if (config.sslCipherSuites != null) {
            builder.cipherSuites(new HashSet<>(Arrays.asList(config.sslCipherSuites.split(","))));
        }

        if (config.sslEnabledProtocols != null) {
            builder.protocols(new HashSet<>(Arrays.asList(config.sslEnabledProtocols.split(","))));
        }

        return builder.build();
    }

    /**
     * Build authentication configuration from agent config.
     */
    protected AuthConfig buildAuthConfig(AgentConfig config) {
        return AuthConfigBuilder.builder()
                .pluginClassName(config.pulsarAuthPluginClassName)
                .authParams(config.pulsarAuthParams)
                .build();
    }

    /**
     * Build batch configuration from agent config.
     */
    protected BatchConfig buildBatchConfig(AgentConfig config) {
        if (config.pulsarBatchDelayInMs <= 0) {
            return BatchConfigBuilder.builder()
                    .enabled(false)
                    .build();
        }
        return BatchConfigBuilder.builder()
                .enabled(true)
                .maxDelayMs(config.pulsarBatchDelayInMs)
                .keyBasedBatching(config.pulsarKeyBasedBatcher)
                .build();
    }

    /**
     * Build routing configuration from agent config.
     */
    protected RoutingConfig buildRoutingConfig(AgentConfig config, boolean useMurmur3) {
        if (!useMurmur3) {
            return null;
        }
        return RoutingConfigBuilder.builder()
                .routingMode(RoutingConfig.RoutingMode.CUSTOM)
                .customRouterClassName(Murmur3MessageRouter.class.getName())
                .build();
    }

    /**
     * Build the message producer for the provided table metadata.
     */
    protected MessageProducer<byte[], MutationValue> getProducer(final TableInfo tm) throws MessagingException {
        if (this.messagingClient == null) {
            synchronized (this) {
                if (this.messagingClient == null)
                    initialize(config);
            }
        }

        final String topicName = config.topicPrefix + tm.key();
        return producers.computeIfAbsent(topicName, k -> {
            try {
                SchemaAndWriter schemaAndWriter = getAvroKeySchema(tm);
                
                // Build key schema definition with NativeSchemaWrapper for Pulsar
                // NativeSchemaWrapper implements org.apache.pulsar.client.api.Schema<byte[]>
                NativeSchemaWrapper pulsarKeySchema = new NativeSchemaWrapper(
                    schemaAndWriter.schema,
                    org.apache.pulsar.common.schema.SchemaType.AVRO
                );
                
                SchemaDefinition keySchema = BaseSchemaDefinition.builder()
                    .type(SchemaType.AVRO)
                    .schemaDefinition(schemaAndWriter.schema.toString())
                    .nativeSchema(pulsarKeySchema)  // Store Pulsar schema, not Avro schema
                    .name(tm.key())
                    .build();
                
                // Build value schema definition (MutationValue as AVRO)
                // Use Pulsar's built-in AVRO schema for MutationValue
                org.apache.pulsar.client.api.Schema<MutationValue> pulsarValueSchema =
                    org.apache.pulsar.client.api.Schema.AVRO(MutationValue.class);
                
                SchemaDefinition valueSchema = BaseSchemaDefinition.builder()
                    .type(SchemaType.AVRO)
                    .schemaDefinition("MutationValue")  // Schema name
                    .nativeSchema(pulsarValueSchema)  // Store Pulsar schema
                    .name("MutationValue")
                    .build();

                // Build producer configuration
                ProducerConfigBuilder<byte[], MutationValue> producerBuilder =
                    ProducerConfigBuilder.<byte[], MutationValue>builder()
                        .topic(k)
                        .producerName("cdc-producer-" + getHostId() + "-" + tm.key())
                        .sendTimeoutMs(30000) // 30 seconds (Pulsar default when timeout=0 means no timeout)
                        .maxPendingMessages(config.pulsarMaxPendingMessages)
                        .blockIfQueueFull(true)
                        .keySchema(keySchema)
                        .valueSchema(valueSchema);

                // Add batch configuration (provider-specific)
                MessagingProvider provider = determineProvider(config);
                if (provider == MessagingProvider.PULSAR) {
                    BatchConfig batchConfig = buildBatchConfig(config);
                    if (batchConfig != null) {
                        producerBuilder.batchConfig(batchConfig);
                    }

                    // Add routing configuration
                    RoutingConfig routingConfig = buildRoutingConfig(config, useMurmur3Partitioner);
                    if (routingConfig != null) {
                        producerBuilder.routingConfig(routingConfig);
                    }

                    log.info("Creating Pulsar producer name={} with batching delay={}ms",
                            "cdc-producer-" + getHostId() + "-" + tm.key(), config.pulsarBatchDelayInMs);
                } else if (provider == MessagingProvider.KAFKA) {
                    // Kafka batching is configured via provider properties
                    log.info("Creating Kafka producer name={} with linger.ms={}ms",
                            "cdc-producer-" + getHostId() + "-" + tm.key(), config.kafkaLingerMs);
                }

                return messagingClient.createProducer(producerBuilder.build());
            } catch (Exception e) {
                log.error("Failed to create producer", e);
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Serialize AVRO generic record to byte array.
     */
    public byte[] serializeAvroGenericRecord(org.apache.avro.generic.GenericRecord genericRecord, 
                                            SpecificDatumWriter<org.apache.avro.generic.GenericRecord> datumWriter) {
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

    /**
     * Build the AVRO key as a GenericRecord.
     */
    public org.apache.avro.generic.GenericRecord buildAvroKey(org.apache.avro.Schema keySchema, AbstractMutation<T> mutation) {
        org.apache.avro.generic.GenericRecord genericRecord = new org.apache.avro.generic.GenericData.Record(keySchema);
        int i = 0;
        for (ColumnInfo columnInfo : mutation.primaryKeyColumns()) {
            if (keySchema.getField(columnInfo.name()) == null)
                throw new CassandraConnectorSchemaException("Not a valid schema field: " + columnInfo.name());
            Object value = cqlToAvro(mutation.getMetadata(), columnInfo.name(), mutation.getPkValues()[i++]);
            // Only put non-null values to ensure optional clustering keys remain null when not present
            if (value != null) {
                genericRecord.put(columnInfo.name(), value);
            }
        }
        return genericRecord;
    }

    @Override
    public CompletableFuture<MessageId> sendMutationAsync(final AbstractMutation<T> mutation) {
        if (!isSupported(mutation)) {
            incSkippedMutations();
            return CompletableFuture.completedFuture(null);
        }
        try {
            MessageProducer<byte[], MutationValue> producer = getProducer(mutation);
            SchemaAndWriter schemaAndWriter = getAvroKeySchema(mutation);

            byte[] keyBytes = serializeAvroGenericRecord(
                    buildAvroKey(schemaAndWriter.schema, mutation),
                    schemaAndWriter.writer);

            Map<String, String> properties = new HashMap<>();
            properties.put(Constants.SEGMENT_AND_POSITION,
                    mutation.getSegment() + ":" + mutation.getPosition());
            properties.put(Constants.TOKEN, mutation.getToken().toString());
            if (mutation.getTs() != -1) {
                properties.put(Constants.WRITETIME, mutation.getTs() + "");
            }

            return producer.sendAsync(keyBytes, mutation.mutationValue(), properties);
        } catch(Exception e) {
            CompletableFuture<MessageId> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        }
    }

    @Override
    public void close() {
        try {
            if (messagingClient != null) {
                synchronized (this) {
                    if (messagingClient != null) {
                        messagingClient.close();
                    }
                }
            }
        } catch (Exception e) {
            log.warn("close failed:", e);
        }
    }
}

