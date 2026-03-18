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
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Maps messaging abstraction configurations to Kafka-specific properties.
 * Handles client, producer, and consumer configuration translation.
 * 
 * <p>Thread-safe utility class.
 */
public final class KafkaConfigMapper {
    
    private static final Logger log = LoggerFactory.getLogger(KafkaConfigMapper.class);
    
    private KafkaConfigMapper() {
        // Utility class
    }
    
    /**
     * Map ClientConfig to Kafka common properties.
     */
    public static Properties mapClientConfig(ClientConfig config) {
        Properties props = new Properties();
        
        // Bootstrap servers (required)
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, config.getServiceUrl());
        
        // Client ID - use provider properties if available
        Object clientId = config.getProviderProperties().get("client.id");
        if (clientId != null) {
            props.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId.toString());
        }
        
        // Connection timeouts
        props.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, 
                  (int) config.getOperationTimeoutMs());
        props.put(CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG, 
                  (int) config.getConnectionTimeoutMs());
        
        // SSL/TLS configuration
        config.getSslConfig().ifPresent(sslConfig -> 
            mapSslConfig(props, sslConfig));
        
        // Authentication configuration
        config.getAuthConfig().ifPresent(authConfig -> 
            mapAuthConfig(props, authConfig));
        
        // Provider-specific properties
        if (config.getProviderProperties() != null) {
            config.getProviderProperties().forEach((key, value) -> 
                props.put(key, value));
        }
        
        log.debug("Mapped client config: {}", props);
        return props;
    }
    
    /**
     * Map ProducerConfig to Kafka producer properties.
     */
    public static <K, V> Properties mapProducerConfig(ClientConfig clientConfig,
                                               com.datastax.oss.cdc.messaging.config.ProducerConfig<K, V> producerConfig) {
        Properties props = new Properties();
        
        // Start with common client properties
        props.putAll(mapClientConfig(clientConfig));
        
        // Key and value serializers (use byte array, schema handling is separate)
        props.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                  ByteArraySerializer.class.getName());
        props.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                  ByteArraySerializer.class.getName());
        
        // Idempotence (enabled by default for exactly-once semantics)
        props.put(org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG, "all");
        props.put(org.apache.kafka.clients.producer.ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        
        // Retries
        props.put(org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);
        
        // Batching configuration
        producerConfig.getBatchConfig().ifPresent(batchConfig ->
            mapBatchConfig(props, batchConfig));
        
        // Compression
        producerConfig.getCompressionType().ifPresent(compressionType ->
            props.put(org.apache.kafka.clients.producer.ProducerConfig.COMPRESSION_TYPE_CONFIG,
                     mapCompressionType(compressionType)));
        
        // Send buffer and queue size
        if (producerConfig.getMaxPendingMessages() > 0) {
            props.put(org.apache.kafka.clients.producer.ProducerConfig.BUFFER_MEMORY_CONFIG,
                     producerConfig.getMaxPendingMessages() * 1024L);
        }
        
        // Routing configuration (partitioner)
        producerConfig.getRoutingConfig().ifPresent(routingConfig ->
            mapRoutingConfig(props, routingConfig));
        
        // Producer-specific provider properties
        if (producerConfig.getProviderProperties() != null) {
            producerConfig.getProviderProperties().forEach((key, value) -> 
                props.put(key, value));
        }
        
        log.debug("Mapped producer config for topic {}: {}", 
                 producerConfig.getTopic(), props);
        return props;
    }
    
    /**
     * Map ConsumerConfig to Kafka consumer properties.
     */
    public static <K, V> Properties mapConsumerConfig(ClientConfig clientConfig,
                                               com.datastax.oss.cdc.messaging.config.ConsumerConfig<K, V> consumerConfig) {
        Properties props = new Properties();
        
        // Start with common client properties
        props.putAll(mapClientConfig(clientConfig));
        
        // Key and value deserializers
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                  ByteArrayDeserializer.class.getName());
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                  ByteArrayDeserializer.class.getName());
        
        // Consumer group and subscription
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG, consumerConfig.getSubscriptionName());
        
        // Auto offset reset
        InitialPosition initialPosition = consumerConfig.getInitialPosition();
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                 mapInitialPosition(initialPosition));
        
        // Manual offset management (disable auto-commit for acknowledgment semantics)
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        
        // Fetch configuration
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
        
        // Partition assignment strategy based on subscription type
        SubscriptionType subscriptionType = consumerConfig.getSubscriptionType();
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                 mapSubscriptionType(subscriptionType));
        
        // Session and heartbeat timeouts
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);
        
        // Consumer-specific provider properties
        if (consumerConfig.getProviderProperties() != null) {
            consumerConfig.getProviderProperties().forEach((key, value) -> 
                props.put(key, value));
        }
        
        log.debug("Mapped consumer config for topic {}: {}",
                 consumerConfig.getTopic(), props);
        return props;
    }
    
    /**
     * Map SSL configuration to Kafka SSL properties.
     */
    private static void mapSslConfig(Properties props, SslConfig sslConfig) {
        if (!sslConfig.isEnabled()) {
            return;
        }
        
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        
        // Truststore
        sslConfig.getTrustStorePath().ifPresent(path ->
            props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, path));
        sslConfig.getTrustStorePassword().ifPresent(password ->
            props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, password));
        sslConfig.getTrustStoreType().ifPresent(type ->
            props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, type));
        
        // Keystore
        sslConfig.getKeyStorePath().ifPresent(path ->
            props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, path));
        sslConfig.getKeyStorePassword().ifPresent(password ->
            props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, password));
        sslConfig.getKeyStoreType().ifPresent(type ->
            props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, type));
        
        // Hostname verification
        if (!sslConfig.isHostnameVerificationEnabled()) {
            props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
        }
        
        // Cipher suites
        sslConfig.getCipherSuites().ifPresent(cipherSuites ->
            props.put(SslConfigs.SSL_CIPHER_SUITES_CONFIG,
                     String.join(",", cipherSuites)));
        
        // Protocols
        sslConfig.getProtocols().ifPresent(protocols ->
            props.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG,
                     String.join(",", protocols)));
    }
    
    /**
     * Map authentication configuration to Kafka SASL properties.
     */
    private static void mapAuthConfig(Properties props, AuthConfig authConfig) {
        String pluginClass = authConfig.getPluginClassName();
        String authParams = authConfig.getAuthParams();
        
        // Determine SASL mechanism from plugin class name
        String mechanism = determineSaslMechanism(pluginClass);
        
        if (mechanism != null) {
            // Update security protocol to include SASL
            String currentProtocol = props.getProperty(
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
            
            if ("SSL".equals(currentProtocol)) {
                props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
            } else {
                props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            }
            
            props.put(SaslConfigs.SASL_MECHANISM, mechanism);
            
            // Build JAAS configuration
            String jaasConfig = buildJaasConfig(mechanism, authParams, authConfig.getProperties());
            if (jaasConfig != null) {
                props.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);
            }
        }
    }
    
    /**
     * Determine SASL mechanism from plugin class name.
     */
    private static String determineSaslMechanism(String pluginClass) {
        if (pluginClass == null) {
            return null;
        }
        
        String lower = pluginClass.toLowerCase();
        if (lower.contains("plain")) {
            return "PLAIN";
        } else if (lower.contains("scram")) {
            return "SCRAM-SHA-512";
        } else if (lower.contains("gssapi") || lower.contains("kerberos")) {
            return "GSSAPI";
        } else if (lower.contains("oauthbearer")) {
            return "OAUTHBEARER";
        }
        
        return null;
    }
    
    /**
     * Build JAAS configuration string from auth parameters.
     */
    private static String buildJaasConfig(String mechanism, String authParams, Map<String, String> properties) {
        if (authParams == null || authParams.isEmpty()) {
            return null;
        }
        
        StringBuilder jaas = new StringBuilder();
        
        // Parse authParams string (format: "username:password" or similar)
        String[] parts = authParams.split(":", 2);
        String username = parts.length > 0 ? parts[0] : "";
        String password = parts.length > 1 ? parts[1] : "";
        
        switch (mechanism) {
            case "PLAIN":
                jaas.append("org.apache.kafka.common.security.plain.PlainLoginModule required ");
                jaas.append("username=\"").append(username).append("\" ");
                jaas.append("password=\"").append(password).append("\";");
                break;
                
            case "SCRAM-SHA-512":
                jaas.append("org.apache.kafka.common.security.scram.ScramLoginModule required ");
                jaas.append("username=\"").append(username).append("\" ");
                jaas.append("password=\"").append(password).append("\";");
                break;
                
            case "GSSAPI":
                jaas.append("com.sun.security.auth.module.Krb5LoginModule required ");
                jaas.append("useKeyTab=true ");
                jaas.append("storeKey=true ");
                String keyTab = properties != null ? properties.getOrDefault("keyTab", "") : "";
                String principal = properties != null ? properties.getOrDefault("principal", username) : username;
                jaas.append("keyTab=\"").append(keyTab).append("\" ");
                jaas.append("principal=\"").append(principal).append("\";");
                break;
                
            default:
                return null;
        }
        
        return jaas.toString();
    }
    
    /**
     * Map batch configuration to Kafka producer properties.
     */
    private static void mapBatchConfig(Properties props, BatchConfig batchConfig) {
        if (batchConfig.getMaxDelayMs() > 0) {
            props.put(org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG,
                     (int) batchConfig.getMaxDelayMs());
        }
        
        if (batchConfig.getMaxMessages() > 0) {
            props.put(org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG,
                     batchConfig.getMaxMessages() * 1024);
        }
        
        if (batchConfig.getMaxBytes() > 0) {
            props.put(org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG,
                     batchConfig.getMaxBytes());
        }
    }
    
    /**
     * Map routing configuration to Kafka partitioner.
     */
    private static void mapRoutingConfig(Properties props, RoutingConfig routingConfig) {
        // Kafka uses key-based partitioning by default
        // Custom partitioner can be specified via provider properties
        if (routingConfig.getRoutingMode() != null) {
            switch (routingConfig.getRoutingMode()) {
                case ROUND_ROBIN:
                    props.put(org.apache.kafka.clients.producer.ProducerConfig.PARTITIONER_CLASS_CONFIG,
                             "org.apache.kafka.clients.producer.RoundRobinPartitioner");
                    break;
                case SINGLE_PARTITION:
                    // Use default partitioner with null key
                    break;
                case CUSTOM:
                    // Custom partitioner should be specified in provider properties
                    break;
            }
        }
    }
    
    /**
     * Map compression type to Kafka compression codec.
     */
    private static String mapCompressionType(CompressionType compressionType) {
        switch (compressionType) {
            case NONE:
                return "none";
            case LZ4:
                return "lz4";
            case ZSTD:
                return "zstd";
            case SNAPPY:
                return "snappy";
            case GZIP:
                return "gzip";
            default:
                return "none";
        }
    }
    
    /**
     * Map initial position to Kafka auto offset reset.
     */
    private static String mapInitialPosition(InitialPosition initialPosition) {
        switch (initialPosition) {
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
     */
    private static String mapSubscriptionType(SubscriptionType subscriptionType) {
        switch (subscriptionType) {
            case EXCLUSIVE:
            case FAILOVER:
                return "org.apache.kafka.clients.consumer.CooperativeStickyAssignor";
            case SHARED:
                return "org.apache.kafka.clients.consumer.RoundRobinAssignor";
            case KEY_SHARED:
                return "org.apache.kafka.clients.consumer.StickyAssignor";
            default:
                return "org.apache.kafka.clients.consumer.RangeAssignor";
        }
    }
}

// Made with Bob
