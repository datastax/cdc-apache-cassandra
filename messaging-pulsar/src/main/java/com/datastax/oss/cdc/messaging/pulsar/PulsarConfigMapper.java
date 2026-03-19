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
import com.datastax.oss.cdc.messaging.MessagingException;
import com.datastax.oss.cdc.messaging.schema.SchemaDefinition;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * Maps messaging abstraction configurations to Pulsar-specific configurations.
 * Provides static utility methods for configuration translation.
 * 
 * <p>Thread-safe utility class.
 */
public final class PulsarConfigMapper {
    
    private static final Logger log = LoggerFactory.getLogger(PulsarConfigMapper.class);
    
    // Private constructor to prevent instantiation
    private PulsarConfigMapper() {
    }
    
    /**
     * Map ClientConfig to Pulsar ClientBuilder.
     * 
     * @param config ClientConfig instance
     * @return Configured ClientBuilder
     * @throws MessagingException if configuration is invalid
     */
    public static ClientBuilder mapClientConfig(ClientConfig config) throws MessagingException {
        try {
            ClientBuilder builder = PulsarClient.builder()
                    .serviceUrl(config.getServiceUrl())
                    .memoryLimit(config.getMemoryLimitBytes(), SizeUnit.BYTES)
                    .operationTimeout((int) config.getOperationTimeoutMs(), TimeUnit.MILLISECONDS)
                    .connectionTimeout((int) config.getConnectionTimeoutMs(), TimeUnit.MILLISECONDS)
                    .enableTcpNoDelay(false);
            
            // Map SSL configuration
            config.getSslConfig().ifPresent(sslConfig -> mapSslConfig(builder, sslConfig));
            
            // Map authentication configuration
            config.getAuthConfig().ifPresent(authConfig -> {
                try {
                    mapAuthConfig(builder, authConfig);
                } catch (PulsarClientException e) {
                    throw new RuntimeException("Failed to configure authentication", e);
                }
            });
            
            return builder;
        } catch (Exception e) {
            throw new MessagingException("Failed to map client configuration", e);
        }
    }
    
    /**
     * Map SSL configuration to ClientBuilder.
     * 
     * @param builder ClientBuilder to configure
     * @param sslConfig SSL configuration
     */
    private static void mapSslConfig(ClientBuilder builder, SslConfig sslConfig) {
        if (!sslConfig.isEnabled()) {
            return;
        }
        
        // Trust store configuration
        sslConfig.getTrustStorePath().ifPresent(builder::tlsTrustStorePath);
        sslConfig.getTrustStorePassword().ifPresent(builder::tlsTrustStorePassword);
        sslConfig.getTrustStoreType().ifPresent(builder::tlsTrustStoreType);
        
        // Key store configuration (for client certificates)
        sslConfig.getKeyStorePath().ifPresent(path -> {
            builder.useKeyStoreTls(true);
            // Pulsar uses trust store path for key store when useKeyStoreTls is true
        });
        
        // PEM certificate configuration (alternative to key/trust stores)
        sslConfig.getTrustedCertificates().ifPresent(builder::tlsTrustCertsFilePath);
        
        // Hostname verification
        builder.enableTlsHostnameVerification(sslConfig.isHostnameVerificationEnabled());
        builder.allowTlsInsecureConnection(!sslConfig.isHostnameVerificationEnabled());
        
        // Cipher suites
        sslConfig.getCipherSuites().ifPresent(builder::tlsCiphers);
        
        // TLS protocols
        sslConfig.getProtocols().ifPresent(builder::tlsProtocols);
    }
    
    /**
     * Map authentication configuration to ClientBuilder.
     * 
     * @param builder ClientBuilder to configure
     * @param authConfig Authentication configuration
     * @throws PulsarClientException if authentication setup fails
     */
    private static void mapAuthConfig(ClientBuilder builder, AuthConfig authConfig) throws PulsarClientException {
        String pluginClassName = authConfig.getPluginClassName();
        String authParams = authConfig.getAuthParams();
        
        if (pluginClassName != null && !pluginClassName.isEmpty()) {
            builder.authentication(pluginClassName, authParams);
            log.debug("Configured Pulsar authentication: plugin={}", pluginClassName);
        }
    }
    
    /**
     * Map ProducerConfig to Pulsar ProducerBuilder.
     * 
     * @param client PulsarClient instance
     * @param config ProducerConfig instance
     * @param <K> Key type
     * @param <V> Value type
     * @return Configured ProducerBuilder
     * @throws MessagingException if configuration is invalid
     */
    public static <K, V> ProducerBuilder<KeyValue<K, V>> mapProducerConfig(
            PulsarClient client, ProducerConfig<K, V> config) throws MessagingException {
        try {
            // Create KeyValue schema from key and value schemas
            Schema<KeyValue<K, V>> keyValueSchema = createKeyValueSchema(
                    config.getKeySchema(), 
                    config.getValueSchema()
            );
            
            ProducerBuilder<KeyValue<K, V>> builder = client.newProducer(keyValueSchema)
                    .topic(config.getTopic())
                    .sendTimeout((int) config.getSendTimeoutMs(), TimeUnit.MILLISECONDS)
                    .maxPendingMessages(config.getMaxPendingMessages())
                    .blockIfQueueFull(config.isBlockIfQueueFull())
                    .hashingScheme(HashingScheme.Murmur3_32Hash)
                    .autoUpdatePartitions(true);
            
            // Set producer name if provided
            config.getProducerName().ifPresent(builder::producerName);
            
            // Map batch configuration
            config.getBatchConfig().ifPresent(batchConfig -> mapBatchConfig(builder, batchConfig));
            
            // Map routing configuration
            config.getRoutingConfig().ifPresent(routingConfig -> mapRoutingConfig(builder, routingConfig));
            
            // Map compression
            config.getCompressionType().ifPresent(compressionType -> 
                    builder.compressionType(mapCompressionType(compressionType)));
            
            return builder;
        } catch (Exception e) {
            throw new MessagingException("Failed to map producer configuration", e);
        }
    }
    
    /**
     * Map batch configuration to ProducerBuilder.
     * 
     * @param builder ProducerBuilder to configure
     * @param batchConfig Batch configuration
     * @param <T> Message type
     */
    private static <T> void mapBatchConfig(ProducerBuilder<T> builder, BatchConfig batchConfig) {
        if (batchConfig.isEnabled()) {
            builder.enableBatching(true)
                    .batchingMaxMessages(batchConfig.getMaxMessages())
                    .batchingMaxBytes(batchConfig.getMaxBytes())
                    .batchingMaxPublishDelay(batchConfig.getMaxDelayMs(), TimeUnit.MILLISECONDS);
            
            if (batchConfig.isKeyBasedBatching()) {
                builder.batcherBuilder(BatcherBuilder.KEY_BASED);
            }
        } else {
            builder.enableBatching(false);
        }
    }
    
    /**
     * Map routing configuration to ProducerBuilder.
     * 
     * @param builder ProducerBuilder to configure
     * @param routingConfig Routing configuration
     * @param <T> Message type
     */
    private static <T> void mapRoutingConfig(ProducerBuilder<T> builder, RoutingConfig routingConfig) {
        RoutingConfig.RoutingMode mode = routingConfig.getRoutingMode();
        
        switch (mode) {
            case ROUND_ROBIN:
                builder.messageRoutingMode(MessageRoutingMode.RoundRobinPartition);
                break;
            case KEY_HASH:
                // Default Pulsar behavior with hashing scheme
                break;
            case SINGLE_PARTITION:
                builder.messageRoutingMode(MessageRoutingMode.SinglePartition);
                break;
            case CUSTOM:
                String customRouter = routingConfig.getCustomRouterClassName();
                if (customRouter != null && !customRouter.isEmpty()) {
                    builder.messageRoutingMode(MessageRoutingMode.CustomPartition);
                    try {
                        @SuppressWarnings("unchecked")
                        Class<? extends MessageRouter> routerClass = 
                                (Class<? extends MessageRouter>) Class.forName(customRouter);
                        MessageRouter router = routerClass.getDeclaredConstructor().newInstance();
                        builder.messageRouter(router);
                    } catch (Exception e) {
                        log.warn("Failed to load custom router: {}", customRouter, e);
                    }
                }
                break;
        }
    }
    
    /**
     * Map compression type to Pulsar CompressionType.
     * 
     * @param compressionType Abstraction compression type
     * @return Pulsar CompressionType
     */
    private static org.apache.pulsar.client.api.CompressionType mapCompressionType(
            com.datastax.oss.cdc.messaging.config.CompressionType compressionType) {
        switch (compressionType) {
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
                return org.apache.pulsar.client.api.CompressionType.NONE;
        }
    }
    
    /**
     * Map ConsumerConfig to Pulsar ConsumerBuilder.
     * 
     * @param client PulsarClient instance
     * @param config ConsumerConfig instance
     * @param <K> Key type
     * @param <V> Value type
     * @return Configured ConsumerBuilder
     * @throws MessagingException if configuration is invalid
     */
    public static <K, V> ConsumerBuilder<KeyValue<K, V>> mapConsumerConfig(
            PulsarClient client, ConsumerConfig<K, V> config) throws MessagingException {
        try {
            // Create KeyValue schema from key and value schemas
            Schema<KeyValue<K, V>> keyValueSchema = createKeyValueSchema(
                    config.getKeySchema(), 
                    config.getValueSchema()
            );
            
            ConsumerBuilder<KeyValue<K, V>> builder = client.newConsumer(keyValueSchema)
                    .topic(config.getTopic())
                    .subscriptionName(config.getSubscriptionName())
                    .subscriptionType(mapSubscriptionType(config.getSubscriptionType()))
                    .subscriptionInitialPosition(mapInitialPosition(config.getInitialPosition()))
                    .receiverQueueSize(config.getReceiverQueueSize())
                    .ackTimeout(config.getAckTimeoutMs(), TimeUnit.MILLISECONDS);
            
            // Set consumer name if provided
            config.getConsumerName().ifPresent(builder::consumerName);
            
            // Auto-acknowledgment is not directly supported in Pulsar
            // Consumers must explicitly acknowledge messages
            
            return builder;
        } catch (Exception e) {
            throw new MessagingException("Failed to map consumer configuration", e);
        }
    }
    
    /**
     * Map subscription type to Pulsar SubscriptionType.
     * 
     * @param subscriptionType Abstraction subscription type
     * @return Pulsar SubscriptionType
     */
    private static org.apache.pulsar.client.api.SubscriptionType mapSubscriptionType(
            com.datastax.oss.cdc.messaging.config.SubscriptionType subscriptionType) {
        switch (subscriptionType) {
            case EXCLUSIVE:
                return org.apache.pulsar.client.api.SubscriptionType.Exclusive;
            case SHARED:
                return org.apache.pulsar.client.api.SubscriptionType.Shared;
            case FAILOVER:
                return org.apache.pulsar.client.api.SubscriptionType.Failover;
            case KEY_SHARED:
                return org.apache.pulsar.client.api.SubscriptionType.Key_Shared;
            default:
                return org.apache.pulsar.client.api.SubscriptionType.Exclusive;
        }
    }
    
    /**
     * Map initial position to Pulsar SubscriptionInitialPosition.
     * 
     * @param initialPosition Abstraction initial position
     * @return Pulsar SubscriptionInitialPosition
     */
    private static SubscriptionInitialPosition mapInitialPosition(InitialPosition initialPosition) {
        switch (initialPosition) {
            case EARLIEST:
                return SubscriptionInitialPosition.Earliest;
            case LATEST:
                return SubscriptionInitialPosition.Latest;
            default:
                return SubscriptionInitialPosition.Latest;
        }
    }
    
    /**
     * Create Pulsar KeyValue schema from key and value schema definitions.
     * 
     * @param keySchema Key schema definition
     * @param valueSchema Value schema definition
     * @param <K> Key type
     * @param <V> Value type
     * @return Pulsar KeyValue schema
     */
    @SuppressWarnings("unchecked")
    private static <K, V> Schema<KeyValue<K, V>> createKeyValueSchema(
            SchemaDefinition keySchema, SchemaDefinition valueSchema) {
        
        // Get native Pulsar schemas from schema definitions
        Schema<K> pulsarKeySchema = (Schema<K>) keySchema.getNativeSchema();
        Schema<V> pulsarValueSchema = (Schema<V>) valueSchema.getNativeSchema();
        
        // Create KeyValue schema with SEPARATED encoding
        // This matches the existing CDC implementation
        return Schema.KeyValue(pulsarKeySchema, pulsarValueSchema, KeyValueEncodingType.SEPARATED);
    }
}

