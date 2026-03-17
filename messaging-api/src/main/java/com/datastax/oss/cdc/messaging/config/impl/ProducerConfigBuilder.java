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
package com.datastax.oss.cdc.messaging.config.impl;

import com.datastax.oss.cdc.messaging.config.BatchConfig;
import com.datastax.oss.cdc.messaging.config.CompressionType;
import com.datastax.oss.cdc.messaging.config.ProducerConfig;
import com.datastax.oss.cdc.messaging.config.RoutingConfig;
import com.datastax.oss.cdc.messaging.schema.SchemaDefinition;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Builder for ProducerConfig.
 * Provides fluent API for constructing immutable producer configuration.
 * 
 * <p>Usage:
 * <pre>{@code
 * ProducerConfig<String, MyData> config = ProducerConfig.<String, MyData>builder()
 *     .topic("my-topic")
 *     .producerName("my-producer")
 *     .keySchema(keySchema)
 *     .valueSchema(valueSchema)
 *     .batchConfig(batchConfig)
 *     .build();
 * }</pre>
 * 
 * @param <K> Key type
 * @param <V> Value type
 */
public class ProducerConfigBuilder<K, V> {
    
    private String topic;
    private String producerName;
    private SchemaDefinition keySchema;
    private SchemaDefinition valueSchema;
    private BatchConfig batchConfig;
    private RoutingConfig routingConfig;
    private int maxPendingMessages = 1000;
    private long sendTimeoutMs = 30000;
    private boolean blockIfQueueFull = true;
    private CompressionType compressionType;
    private Map<String, Object> providerProperties = new HashMap<>();
    
    private ProducerConfigBuilder() {
    }
    
    /**
     * Create a new builder.
     * 
     * @param <K> Key type
     * @param <V> Value type
     * @return Builder instance
     */
    public static <K, V> ProducerConfigBuilder<K, V> builder() {
        return new ProducerConfigBuilder<>();
    }
    
    /**
     * Set topic name.
     * 
     * @param topic Topic name
     * @return This builder
     */
    public ProducerConfigBuilder<K, V> topic(String topic) {
        this.topic = topic;
        return this;
    }
    
    /**
     * Set producer name.
     * 
     * @param producerName Producer name
     * @return This builder
     */
    public ProducerConfigBuilder<K, V> producerName(String producerName) {
        this.producerName = producerName;
        return this;
    }
    
    /**
     * Set key schema.
     * 
     * @param keySchema Key schema definition
     * @return This builder
     */
    public ProducerConfigBuilder<K, V> keySchema(SchemaDefinition keySchema) {
        this.keySchema = keySchema;
        return this;
    }
    
    /**
     * Set value schema.
     * 
     * @param valueSchema Value schema definition
     * @return This builder
     */
    public ProducerConfigBuilder<K, V> valueSchema(SchemaDefinition valueSchema) {
        this.valueSchema = valueSchema;
        return this;
    }
    
    /**
     * Set batch configuration.
     * 
     * @param batchConfig Batch configuration
     * @return This builder
     */
    public ProducerConfigBuilder<K, V> batchConfig(BatchConfig batchConfig) {
        this.batchConfig = batchConfig;
        return this;
    }
    
    /**
     * Set routing configuration.
     * 
     * @param routingConfig Routing configuration
     * @return This builder
     */
    public ProducerConfigBuilder<K, V> routingConfig(RoutingConfig routingConfig) {
        this.routingConfig = routingConfig;
        return this;
    }
    
    /**
     * Set max pending messages.
     * 
     * @param maxPendingMessages Max pending messages (must be > 0)
     * @return This builder
     */
    public ProducerConfigBuilder<K, V> maxPendingMessages(int maxPendingMessages) {
        if (maxPendingMessages <= 0) {
            throw new IllegalArgumentException("Max pending messages must be > 0");
        }
        this.maxPendingMessages = maxPendingMessages;
        return this;
    }
    
    /**
     * Set send timeout.
     * 
     * @param sendTimeoutMs Send timeout in milliseconds (must be > 0)
     * @return This builder
     */
    public ProducerConfigBuilder<K, V> sendTimeoutMs(long sendTimeoutMs) {
        if (sendTimeoutMs <= 0) {
            throw new IllegalArgumentException("Send timeout must be > 0");
        }
        this.sendTimeoutMs = sendTimeoutMs;
        return this;
    }
    
    /**
     * Set block if queue full.
     * 
     * @param blockIfQueueFull true to block, false to fail immediately
     * @return This builder
     */
    public ProducerConfigBuilder<K, V> blockIfQueueFull(boolean blockIfQueueFull) {
        this.blockIfQueueFull = blockIfQueueFull;
        return this;
    }
    
    /**
     * Set compression type.
     * 
     * @param compressionType Compression type
     * @return This builder
     */
    public ProducerConfigBuilder<K, V> compressionType(CompressionType compressionType) {
        this.compressionType = compressionType;
        return this;
    }
    
    /**
     * Set provider-specific properties.
     * 
     * @param providerProperties Properties map
     * @return This builder
     */
    public ProducerConfigBuilder<K, V> providerProperties(Map<String, Object> providerProperties) {
        if (providerProperties != null) {
            this.providerProperties.putAll(providerProperties);
        }
        return this;
    }
    
    /**
     * Add a single provider property.
     * 
     * @param key Property key
     * @param value Property value
     * @return This builder
     */
    public ProducerConfigBuilder<K, V> providerProperty(String key, Object value) {
        if (key != null && value != null) {
            this.providerProperties.put(key, value);
        }
        return this;
    }
    
    /**
     * Build the ProducerConfig.
     * 
     * @return Immutable ProducerConfig instance
     * @throws IllegalStateException if required fields are missing
     */
    public ProducerConfig<K, V> build() {
        if (topic == null || topic.isEmpty()) {
            throw new IllegalStateException("Topic is required");
        }
        if (keySchema == null) {
            throw new IllegalStateException("Key schema is required");
        }
        if (valueSchema == null) {
            throw new IllegalStateException("Value schema is required");
        }
        return new ProducerConfigImpl<>(
            topic, producerName, keySchema, valueSchema, batchConfig, routingConfig,
            maxPendingMessages, sendTimeoutMs, blockIfQueueFull, compressionType,
            providerProperties
        );
    }
    
    /**
     * Immutable implementation of ProducerConfig.
     */
    private static class ProducerConfigImpl<K, V> implements ProducerConfig<K, V> {
        private final String topic;
        private final String producerName;
        private final SchemaDefinition keySchema;
        private final SchemaDefinition valueSchema;
        private final BatchConfig batchConfig;
        private final RoutingConfig routingConfig;
        private final int maxPendingMessages;
        private final long sendTimeoutMs;
        private final boolean blockIfQueueFull;
        private final CompressionType compressionType;
        private final Map<String, Object> providerProperties;
        
        ProducerConfigImpl(String topic, String producerName,
                          SchemaDefinition keySchema, SchemaDefinition valueSchema,
                          BatchConfig batchConfig, RoutingConfig routingConfig,
                          int maxPendingMessages, long sendTimeoutMs,
                          boolean blockIfQueueFull, CompressionType compressionType,
                          Map<String, Object> providerProperties) {
            this.topic = topic;
            this.producerName = producerName;
            this.keySchema = keySchema;
            this.valueSchema = valueSchema;
            this.batchConfig = batchConfig;
            this.routingConfig = routingConfig;
            this.maxPendingMessages = maxPendingMessages;
            this.sendTimeoutMs = sendTimeoutMs;
            this.blockIfQueueFull = blockIfQueueFull;
            this.compressionType = compressionType;
            this.providerProperties = Collections.unmodifiableMap(new HashMap<>(providerProperties));
        }
        
        @Override
        public String getTopic() {
            return topic;
        }
        
        @Override
        public Optional<String> getProducerName() {
            return Optional.ofNullable(producerName);
        }
        
        @Override
        public SchemaDefinition getKeySchema() {
            return keySchema;
        }
        
        @Override
        public SchemaDefinition getValueSchema() {
            return valueSchema;
        }
        
        @Override
        public Optional<BatchConfig> getBatchConfig() {
            return Optional.ofNullable(batchConfig);
        }
        
        @Override
        public Optional<RoutingConfig> getRoutingConfig() {
            return Optional.ofNullable(routingConfig);
        }
        
        @Override
        public int getMaxPendingMessages() {
            return maxPendingMessages;
        }
        
        @Override
        public long getSendTimeoutMs() {
            return sendTimeoutMs;
        }
        
        @Override
        public boolean isBlockIfQueueFull() {
            return blockIfQueueFull;
        }
        
        @Override
        public Optional<CompressionType> getCompressionType() {
            return Optional.ofNullable(compressionType);
        }
        
        @Override
        public Map<String, Object> getProviderProperties() {
            return providerProperties;
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ProducerConfigImpl<?, ?> that = (ProducerConfigImpl<?, ?>) o;
            return maxPendingMessages == that.maxPendingMessages &&
                   sendTimeoutMs == that.sendTimeoutMs &&
                   blockIfQueueFull == that.blockIfQueueFull &&
                   Objects.equals(topic, that.topic) &&
                   Objects.equals(producerName, that.producerName) &&
                   Objects.equals(keySchema, that.keySchema) &&
                   Objects.equals(valueSchema, that.valueSchema) &&
                   Objects.equals(batchConfig, that.batchConfig) &&
                   Objects.equals(routingConfig, that.routingConfig) &&
                   compressionType == that.compressionType &&
                   Objects.equals(providerProperties, that.providerProperties);
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(topic, producerName, keySchema, valueSchema, batchConfig,
                routingConfig, maxPendingMessages, sendTimeoutMs, blockIfQueueFull,
                compressionType, providerProperties);
        }
        
        @Override
        public String toString() {
            return "ProducerConfig{" +
                    "topic='" + topic + '\'' +
                    ", producerName='" + producerName + '\'' +
                    ", maxPendingMessages=" + maxPendingMessages +
                    ", sendTimeoutMs=" + sendTimeoutMs +
                    ", blockIfQueueFull=" + blockIfQueueFull +
                    ", compressionType=" + compressionType +
                    '}';
        }
    }
}

// Made with Bob