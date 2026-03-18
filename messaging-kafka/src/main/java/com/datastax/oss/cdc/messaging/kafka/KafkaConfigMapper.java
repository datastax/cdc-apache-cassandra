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
package com.datastax.oss.cdc.messaging.kafka;

import com.datastax.oss.cdc.messaging.config.*;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Maps messaging abstraction configurations to Kafka-specific properties.
 * Handles conversion of generic config to Kafka producer/consumer properties.
 */
public class KafkaConfigMapper {
    
    private static final Logger log = LoggerFactory.getLogger(KafkaConfigMapper.class);
    
    /**
     * Build common Kafka properties from ClientConfig.
     * These properties are shared between producers and consumers.
     *
     * @param config Client configuration
     * @return Kafka properties
     */
    public static Properties buildCommonProperties(ClientConfig config) {
        log.debug("Building common Kafka properties for bootstrap servers: {}", config.getServiceUrl());
        
        Properties props = new Properties();
        
        // Bootstrap servers (service URL in our abstraction)
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, config.getServiceUrl());
        
        // Connection timeout
        if (config.getConnectionTimeoutMs() > 0) {
            props.put(CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG, 
                config.getConnectionTimeoutMs());
        }
        
        // Request timeout
        if (config.getOperationTimeoutMs() > 0) {
            props.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, 
                (int) config.getOperationTimeoutMs());
        }
        
        // SSL configuration
        config.getSslConfig().ifPresent(ssl -> mapSslConfig(props, ssl));
        
        // Authentication configuration
        config.getAuthConfig().ifPresent(auth -> mapAuthConfig(props, auth));
        
        // Client ID from provider properties if available
        Object clientId = config.getProviderProperties().get("client.id");
        if (clientId != null) {
            props.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId.toString());
        }
        
        return props;
    }
    
    /**
     * Map SSL configuration to Kafka properties.
     * 
     * @param props Kafka properties
     * @param sslConfig SSL configuration
     */
    private static void mapSslConfig(Properties props, SslConfig sslConfig) {
        log.debug("Mapping SSL configuration");
        
        // Enable SSL
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        
        // Trust store configuration
        sslConfig.getTrustStorePath().ifPresent(path -> 
            props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, path));
        sslConfig.getTrustStorePassword().ifPresent(password -> 
            props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, password));
        sslConfig.getTrustStoreType().ifPresent(type -> 
            props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, type));
        
        // Key store configuration
        sslConfig.getKeyStorePath().ifPresent(path -> 
            props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, path));
        sslConfig.getKeyStorePassword().ifPresent(password -> 
            props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, password));
        sslConfig.getKeyStoreType().ifPresent(type -> 
            props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, type));
        
        // Hostname verification
        if (sslConfig.isHostnameVerificationEnabled()) {
            props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "https");
        } else {
            props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
        }
        
        // Cipher suites
        sslConfig.getCipherSuites().ifPresent(ciphers -> {
            if (!ciphers.isEmpty()) {
                props.put(SslConfigs.SSL_CIPHER_SUITES_CONFIG, String.join(",", ciphers));
            }
        });
        
        // TLS protocols
        sslConfig.getProtocols().ifPresent(protocols -> {
            if (!protocols.isEmpty()) {
                props.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, String.join(",", protocols));
            }
        });
    }
    
    /**
     * Map authentication configuration to Kafka properties.
     * Supports SASL authentication mechanisms.
     *
     * @param props Kafka properties
     * @param authConfig Authentication configuration
     */
    private static void mapAuthConfig(Properties props, AuthConfig authConfig) {
        log.debug("Mapping authentication configuration");
        
        // Kafka uses SASL for authentication
        // The plugin class name maps to SASL mechanism
        if (authConfig.getPluginClassName() != null) {
            String mechanism = authConfig.getPluginClassName();
            
            // Map common authentication types
            if (mechanism.contains("PLAIN")) {
                props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
                props.put("sasl.mechanism", "PLAIN");
            } else if (mechanism.contains("SCRAM")) {
                props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
                props.put("sasl.mechanism", "SCRAM-SHA-512");
            } else if (mechanism.contains("GSSAPI") || mechanism.contains("Kerberos")) {
                props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
                props.put("sasl.mechanism", "GSSAPI");
            }
            
            // SASL JAAS config from auth params
            if (authConfig.getAuthParams() != null && !authConfig.getAuthParams().isEmpty()) {
                props.put("sasl.jaas.config", authConfig.getAuthParams());
            }
        }
    }
    
    /**
     * Build Kafka producer properties from ProducerConfig.
     *
     * @param commonProps Common Kafka properties
     * @param config Producer configuration
     * @return Kafka producer properties
     */
    public static Properties buildProducerProperties(Properties commonProps,
            com.datastax.oss.cdc.messaging.config.ProducerConfig<?, ?> config) {
        log.debug("Building Kafka producer properties for topic: {}", config.getTopic());
        
        Properties props = new Properties();
        props.putAll(commonProps);
        
        // Serializers - use byte array, actual serialization handled by schema provider
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        
        // Producer name
        if (config.getProducerName() != null && !config.getProducerName().isEmpty()) {
            props.put(ProducerConfig.CLIENT_ID_CONFIG, config.getProducerName());
        }
        
        // Batching configuration
        config.getBatchConfig().ifPresent(batch -> mapBatchConfig(props, batch));
        
        // Compression
        config.getCompressionType().ifPresent(compression -> 
            mapCompressionType(props, compression));
        
        // Send timeout
        if (config.getSendTimeoutMs() > 0) {
            props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, (int) config.getSendTimeoutMs());
        }
        
        // Idempotence - enable by default for exactly-once semantics
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        
        return props;
    }
    
    /**
     * Map batch configuration to Kafka producer properties.
     * 
     * @param props Kafka properties
     * @param batchConfig Batch configuration
     */
    private static void mapBatchConfig(Properties props, BatchConfig batchConfig) {
        log.debug("Mapping batch configuration");
        
        // Batch size in bytes
        if (batchConfig.getMaxBytes() > 0) {
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchConfig.getMaxBytes());
        }
        
        // Linger time (max delay)
        if (batchConfig.getMaxDelayMs() > 0) {
            props.put(ProducerConfig.LINGER_MS_CONFIG, (int) batchConfig.getMaxDelayMs());
        }
        
        // Max messages per batch - Kafka doesn't have direct equivalent
        // This is controlled by batch size and linger time
    }
    
    /**
     * Map compression type to Kafka producer properties.
     * 
     * @param props Kafka properties
     * @param compressionType Compression type
     */
    private static void mapCompressionType(Properties props, CompressionType compressionType) {
        String kafkaCompression;
        switch (compressionType) {
            case NONE:
                kafkaCompression = "none";
                break;
            case LZ4:
                kafkaCompression = "lz4";
                break;
            case ZLIB:
                kafkaCompression = "gzip"; // Kafka uses gzip for zlib
                break;
            case ZSTD:
                kafkaCompression = "zstd";
                break;
            case SNAPPY:
                kafkaCompression = "snappy";
                break;
            default:
                kafkaCompression = "none";
        }
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, kafkaCompression);
    }
    
    /**
     * Build Kafka consumer properties from ConsumerConfig.
     *
     * @param commonProps Common Kafka properties
     * @param config Consumer configuration
     * @return Kafka consumer properties
     */
    public static Properties buildConsumerProperties(Properties commonProps,
            com.datastax.oss.cdc.messaging.config.ConsumerConfig<?, ?> config) {
        log.debug("Building Kafka consumer properties for topic: {}, subscription: {}", 
            config.getTopic(), config.getSubscriptionName());
        
        Properties props = new Properties();
        props.putAll(commonProps);
        
        // Deserializers - use byte array, actual deserialization handled by schema provider
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        
        // Consumer group (subscription name in our abstraction)
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getSubscriptionName());
        
        // Auto offset reset
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, 
            mapInitialPosition(config.getInitialPosition()));
        
        // Disable auto commit - we handle acknowledgment manually
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        
        // Receive queue size
        if (config.getReceiverQueueSize() > 0) {
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, config.getReceiverQueueSize());
        }
        
        // Consumer name
        if (config.getConsumerName() != null && !config.getConsumerName().isEmpty()) {
            props.put(ConsumerConfig.CLIENT_ID_CONFIG, config.getConsumerName());
        }
        
        // Map subscription type to partition assignment strategy
        mapSubscriptionType(props, config.getSubscriptionType());
        
        return props;
    }
    
    /**
     * Map initial position to Kafka auto offset reset.
     * 
     * @param position Initial position
     * @return Kafka auto offset reset value
     */
    private static String mapInitialPosition(InitialPosition position) {
        switch (position) {
            case EARLIEST:
                return "earliest";
            case LATEST:
                return "latest";
            default:
                return "latest";
        }
    }
    
    /**
     * Map subscription type to Kafka partition assignment strategy.
     * 
     * @param props Kafka properties
     * @param subscriptionType Subscription type
     */
    private static void mapSubscriptionType(Properties props, SubscriptionType subscriptionType) {
        String strategy;
        switch (subscriptionType) {
            case EXCLUSIVE:
            case FAILOVER:
                // Use cooperative sticky for failover behavior
                strategy = "org.apache.kafka.clients.consumer.CooperativeStickyAssignor";
                break;
            case SHARED:
                // Use round robin for shared behavior
                strategy = "org.apache.kafka.clients.consumer.RoundRobinAssignor";
                break;
            case KEY_SHARED:
                // Use sticky assignor for key-based routing
                strategy = "org.apache.kafka.clients.consumer.StickyAssignor";
                break;
            default:
                strategy = "org.apache.kafka.clients.consumer.RangeAssignor";
        }
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, strategy);
    }
}

// Made with Bob