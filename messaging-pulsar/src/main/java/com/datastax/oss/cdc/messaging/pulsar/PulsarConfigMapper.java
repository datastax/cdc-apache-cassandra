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
package com.datastax.oss.cdc.messaging.pulsar;

import com.datastax.oss.cdc.messaging.config.*;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * Maps messaging abstraction configurations to Pulsar-specific configurations.
 * Handles conversion of generic config to Pulsar ClientBuilder, ProducerBuilder, ConsumerBuilder.
 */
public class PulsarConfigMapper {
    
    private static final Logger log = LoggerFactory.getLogger(PulsarConfigMapper.class);
    
    /**
     * Map ClientConfig to Pulsar ClientBuilder.
     *
     * @param config Client configuration
     * @return Pulsar ClientBuilder
     * @throws Exception if configuration mapping fails
     */
    public static ClientBuilder mapClientConfig(ClientConfig config) throws Exception {
        log.debug("Mapping client config for service URL: {}", config.getServiceUrl());
        
        ClientBuilder builder = PulsarClient.builder()
            .serviceUrl(config.getServiceUrl());
        
        // Memory limit
        if (config.getMemoryLimitBytes() > 0) {
            builder.memoryLimit(config.getMemoryLimitBytes(), SizeUnit.BYTES);
        }
        
        // Connection timeout
        if (config.getConnectionTimeoutMs() > 0) {
            builder.connectionTimeout((int) config.getConnectionTimeoutMs(), TimeUnit.MILLISECONDS);
        }
        
        // Operation timeout
        if (config.getOperationTimeoutMs() > 0) {
            builder.operationTimeout((int) config.getOperationTimeoutMs(), TimeUnit.MILLISECONDS);
        }
        
        // TCP no delay
        builder.enableTcpNoDelay(false);
        
        // SSL configuration
        config.getSslConfig().ifPresent(ssl -> mapSslConfig(builder, ssl));
        
        // Authentication configuration
        if (config.getAuthConfig().isPresent()) {
            mapAuthConfig(builder, config.getAuthConfig().get());
        }
        
        return builder;
    }
    
    /**
     * Map SSL configuration to ClientBuilder.
     * 
     * @param builder Client builder
     * @param sslConfig SSL configuration
     */
    private static void mapSslConfig(ClientBuilder builder, SslConfig sslConfig) {
        log.debug("Mapping SSL configuration");
        
        // Trust store configuration
        sslConfig.getTrustStorePath().ifPresent(builder::tlsTrustStorePath);
        sslConfig.getTrustStorePassword().ifPresent(builder::tlsTrustStorePassword);
        sslConfig.getTrustStoreType().ifPresent(builder::tlsTrustStoreType);
        
        // Trusted certificates (PEM format)
        sslConfig.getTrustedCertificates().ifPresent(builder::tlsTrustCertsFilePath);
        
        // Key store configuration
        sslConfig.getKeyStorePath().ifPresent(path -> {
            builder.useKeyStoreTls(true);
            builder.tlsKeyStorePath(path);
        });
        sslConfig.getKeyStorePassword().ifPresent(builder::tlsKeyStorePassword);
        sslConfig.getKeyStoreType().ifPresent(builder::tlsKeyStoreType);
        
        // Hostname verification
        builder.enableTlsHostnameVerification(sslConfig.isHostnameVerificationEnabled());
        
        // Cipher suites
        sslConfig.getCipherSuites().ifPresent(ciphers -> {
            if (!ciphers.isEmpty()) {
                builder.tlsCiphers(ciphers);
            }
        });
        
        // TLS protocols
        sslConfig.getProtocols().ifPresent(protocols -> {
            if (!protocols.isEmpty()) {
                builder.tlsProtocols(protocols);
            }
        });
    }
    
    /**
     * Map authentication configuration to ClientBuilder.
     *
     * @param builder Client builder
     * @param authConfig Authentication configuration
     * @throws Exception if authentication configuration fails
     */
    private static void mapAuthConfig(ClientBuilder builder, AuthConfig authConfig) throws Exception {
        log.debug("Mapping authentication configuration");
        
        if (authConfig.getPluginClassName() != null) {
            builder.authentication(authConfig.getPluginClassName(), authConfig.getAuthParams());
        }
    }
    
    /**
     * Map ProducerConfig to Pulsar ProducerBuilder.
     * 
     * @param <K> Key type
     * @param <V> Value type
     * @param client Pulsar client
     * @param config Producer configuration
     * @return Pulsar ProducerBuilder
     */
    public static <K, V> ProducerBuilder<KeyValue<K, V>> mapProducerConfig(
            PulsarClient client, ProducerConfig<K, V> config) {
        log.debug("Mapping producer config for topic: {}", config.getTopic());
        
        // Create key-value schema
        Schema<KeyValue<K, V>> schema = createKeyValueSchema(config);
        
        ProducerBuilder<KeyValue<K, V>> builder = client.newProducer(schema)
            .topic(config.getTopic());
        
        // Producer name
        config.getProducerName().ifPresent(builder::producerName);
        
        // Send timeout
        if (config.getSendTimeoutMs() > 0) {
            builder.sendTimeout((int) config.getSendTimeoutMs(), TimeUnit.MILLISECONDS);
        }
        
        // Max pending messages
        if (config.getMaxPendingMessages() > 0) {
            builder.maxPendingMessages(config.getMaxPendingMessages());
        }
        
        // Block if queue full
        builder.blockIfQueueFull(config.isBlockIfQueueFull());
        
        // Batch configuration
        config.getBatchConfig().ifPresent(batch -> mapBatchConfig(builder, batch));
        
        // Routing configuration
        config.getRoutingConfig().ifPresent(routing -> mapRoutingConfig(builder, routing));
        
        // Compression
        config.getCompressionType().ifPresent(compression ->
            builder.compressionType(mapCompressionType(compression)));
        
        return builder;
    }
    
    /**
     * Map batch configuration to ProducerBuilder.
     * 
     * @param <K> Key type
     * @param <V> Value type
     * @param builder Producer builder
     * @param batchConfig Batch configuration
     */
    private static <K, V> void mapBatchConfig(
            ProducerBuilder<KeyValue<K, V>> builder, BatchConfig batchConfig) {
        log.debug("Mapping batch configuration");
        
        builder.enableBatching(batchConfig.isEnabled());
        
        if (batchConfig.getMaxMessages() > 0) {
            builder.batchingMaxMessages(batchConfig.getMaxMessages());
        }
        
        if (batchConfig.getMaxDelayMs() > 0) {
            builder.batchingMaxPublishDelay(batchConfig.getMaxDelayMs(), TimeUnit.MILLISECONDS);
        }
        
        // Key-based batching
        if (batchConfig.isKeyBasedBatching()) {
            builder.batcherBuilder(BatcherBuilder.KEY_BASED);
        }
    }
    
    /**
     * Map routing configuration to ProducerBuilder.
     * 
     * @param <K> Key type
     * @param <V> Value type
     * @param builder Producer builder
     * @param routingConfig Routing configuration
     */
    private static <K, V> void mapRoutingConfig(
            ProducerBuilder<KeyValue<K, V>> builder, RoutingConfig routingConfig) {
        log.debug("Mapping routing configuration");
        
        // Routing mode
        RoutingConfig.RoutingMode mode = routingConfig.getRoutingMode();
        if (mode != null) {
            switch (mode) {
                case SINGLE_PARTITION:
                    builder.messageRoutingMode(MessageRoutingMode.SinglePartition);
                    break;
                case ROUND_ROBIN:
                    builder.messageRoutingMode(MessageRoutingMode.RoundRobinPartition);
                    break;
                case CUSTOM:
                    builder.messageRoutingMode(MessageRoutingMode.CustomPartition);
                    break;
                case KEY_HASH:
                    // Key hash is default behavior in Pulsar
                    break;
            }
        }
    }
    
    /**
     * Map ConsumerConfig to Pulsar ConsumerBuilder.
     * 
     * @param <K> Key type
     * @param <V> Value type
     * @param client Pulsar client
     * @param config Consumer configuration
     * @return Pulsar ConsumerBuilder
     */
    public static <K, V> ConsumerBuilder<KeyValue<K, V>> mapConsumerConfig(
            PulsarClient client, ConsumerConfig<K, V> config) {
        log.debug("Mapping consumer config for topic: {}, subscription: {}",
            config.getTopic(), config.getSubscriptionName());
        
        // Create key-value schema
        Schema<KeyValue<K, V>> schema = createKeyValueSchema(config);
        
        ConsumerBuilder<KeyValue<K, V>> builder = client.newConsumer(schema)
            .topic(config.getTopic())
            .subscriptionName(config.getSubscriptionName())
            .subscriptionType(mapSubscriptionType(config.getSubscriptionType()));
        
        // Consumer name
        config.getConsumerName().ifPresent(builder::consumerName);
        
        // Initial position
        builder.subscriptionInitialPosition(mapInitialPosition(config.getInitialPosition()));
        
        // Acknowledgment timeout
        if (config.getAckTimeoutMs() > 0) {
            builder.ackTimeout(config.getAckTimeoutMs(), TimeUnit.MILLISECONDS);
        }
        
        return builder;
    }
    
    /**
     * Map subscription type to Pulsar SubscriptionType.
     * 
     * @param type Subscription type
     * @return Pulsar SubscriptionType
     */
    private static org.apache.pulsar.client.api.SubscriptionType mapSubscriptionType(
            com.datastax.oss.cdc.messaging.config.SubscriptionType type) {
        switch (type) {
            case EXCLUSIVE:
                return org.apache.pulsar.client.api.SubscriptionType.Exclusive;
            case SHARED:
                return org.apache.pulsar.client.api.SubscriptionType.Shared;
            case FAILOVER:
                return org.apache.pulsar.client.api.SubscriptionType.Failover;
            case KEY_SHARED:
                return org.apache.pulsar.client.api.SubscriptionType.Key_Shared;
            default:
                throw new IllegalArgumentException("Unknown subscription type: " + type);
        }
    }
    
    /**
     * Map initial position to Pulsar SubscriptionInitialPosition.
     * 
     * @param position Initial position
     * @return Pulsar SubscriptionInitialPosition
     */
    private static SubscriptionInitialPosition mapInitialPosition(InitialPosition position) {
        switch (position) {
            case EARLIEST:
                return SubscriptionInitialPosition.Earliest;
            case LATEST:
                return SubscriptionInitialPosition.Latest;
            default:
                throw new IllegalArgumentException("Unknown initial position: " + position);
        }
    }
    
    /**
     * Map compression type to Pulsar CompressionType.
     * 
     * @param type Compression type
     * @return Pulsar CompressionType
     */
    private static org.apache.pulsar.client.api.CompressionType mapCompressionType(
            com.datastax.oss.cdc.messaging.config.CompressionType type) {
        switch (type) {
            case NONE:
                return org.apache.pulsar.client.api.CompressionType.NONE;
            case LZ4:
                return org.apache.pulsar.client.api.CompressionType.LZ4;
            case ZLIB:
                return org.apache.pulsar.client.api.CompressionType.ZLIB;
            case ZSTD:
                return org.apache.pulsar.client.api.CompressionType.ZSTD;
            case SNAPPY:
                return org.apache.pulsar.client.api.CompressionType.SNAPPY;
            default:
                throw new IllegalArgumentException("Unknown compression type: " + type);
        }
    }
    
    /**
     * Create key-value schema for producer/consumer.
     * Uses BYTES schema for both key and value to support any type.
     *
     * @param <K> Key type
     * @param <V> Value type
     * @param config Configuration with schema info
     * @return Key-value schema
     */
    @SuppressWarnings("unchecked")
    private static <K, V> Schema<KeyValue<K, V>> createKeyValueSchema(Object config) {
        // For now, use BYTES schema which supports any serializable type
        // In future, can be enhanced to use schema registry
        Schema<KeyValue<byte[], byte[]>> bytesSchema = Schema.KeyValue(
            Schema.BYTES,
            Schema.BYTES,
            KeyValueEncodingType.SEPARATED
        );
        return (Schema<KeyValue<K, V>>) (Schema<?>) bytesSchema;
    }
}

// Made with Bob
