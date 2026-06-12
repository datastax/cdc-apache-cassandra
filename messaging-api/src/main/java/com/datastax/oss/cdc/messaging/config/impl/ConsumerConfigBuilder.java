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

import com.datastax.oss.cdc.messaging.config.ConsumerConfig;
import com.datastax.oss.cdc.messaging.config.InitialPosition;
import com.datastax.oss.cdc.messaging.config.SubscriptionType;
import com.datastax.oss.cdc.messaging.schema.SchemaDefinition;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Builder for ConsumerConfig.
 * Provides fluent API for constructing immutable consumer configuration.
 * 
 * <p>Usage:
 * <pre>{@code
 * ConsumerConfig<String, MyData> config = ConsumerConfig.<String, MyData>builder()
 *     .topic("my-topic")
 *     .subscriptionName("my-subscription")
 *     .subscriptionType(SubscriptionType.KEY_SHARED)
 *     .keySchema(keySchema)
 *     .valueSchema(valueSchema)
 *     .build();
 * }</pre>
 * 
 * @param <K> Key type
 * @param <V> Value type
 */
public class ConsumerConfigBuilder<K, V> {
    
    private String topic;
    private String subscriptionName;
    private SubscriptionType subscriptionType = SubscriptionType.EXCLUSIVE;
    private String consumerName;
    private SchemaDefinition keySchema;
    private SchemaDefinition valueSchema;
    private InitialPosition initialPosition = InitialPosition.LATEST;
    private int receiverQueueSize = 1000;
    private long ackTimeoutMs = 0; // 0 = disabled
    private boolean autoAcknowledge = false;
    private Map<String, Object> providerProperties = new HashMap<>();
    
    private ConsumerConfigBuilder() {
    }
    
    /**
     * Create a new builder.
     * 
     * @param <K> Key type
     * @param <V> Value type
     * @return Builder instance
     */
    public static <K, V> ConsumerConfigBuilder<K, V> builder() {
        return new ConsumerConfigBuilder<>();
    }
    
    /**
     * Set topic name or pattern.
     * 
     * @param topic Topic name/pattern
     * @return This builder
     */
    public ConsumerConfigBuilder<K, V> topic(String topic) {
        this.topic = topic;
        return this;
    }
    
    /**
     * Set subscription name.
     * 
     * @param subscriptionName Subscription name
     * @return This builder
     */
    public ConsumerConfigBuilder<K, V> subscriptionName(String subscriptionName) {
        this.subscriptionName = subscriptionName;
        return this;
    }
    
    /**
     * Set subscription type.
     * 
     * @param subscriptionType Subscription type
     * @return This builder
     */
    public ConsumerConfigBuilder<K, V> subscriptionType(SubscriptionType subscriptionType) {
        this.subscriptionType = subscriptionType;
        return this;
    }
    
    /**
     * Set consumer name.
     * 
     * @param consumerName Consumer name
     * @return This builder
     */
    public ConsumerConfigBuilder<K, V> consumerName(String consumerName) {
        this.consumerName = consumerName;
        return this;
    }
    
    /**
     * Set key schema.
     * 
     * @param keySchema Key schema definition
     * @return This builder
     */
    public ConsumerConfigBuilder<K, V> keySchema(SchemaDefinition keySchema) {
        this.keySchema = keySchema;
        return this;
    }
    
    /**
     * Set value schema.
     * 
     * @param valueSchema Value schema definition
     * @return This builder
     */
    public ConsumerConfigBuilder<K, V> valueSchema(SchemaDefinition valueSchema) {
        this.valueSchema = valueSchema;
        return this;
    }
    
    /**
     * Set initial position for new subscription.
     * 
     * @param initialPosition Initial position
     * @return This builder
     */
    public ConsumerConfigBuilder<K, V> initialPosition(InitialPosition initialPosition) {
        this.initialPosition = initialPosition;
        return this;
    }
    
    /**
     * Set receiver queue size.
     * 
     * @param receiverQueueSize Queue size (must be > 0)
     * @return This builder
     */
    public ConsumerConfigBuilder<K, V> receiverQueueSize(int receiverQueueSize) {
        if (receiverQueueSize <= 0) {
            throw new IllegalArgumentException("Receiver queue size must be > 0");
        }
        this.receiverQueueSize = receiverQueueSize;
        return this;
    }
    
    /**
     * Set acknowledgment timeout.
     * 
     * @param ackTimeoutMs Timeout in milliseconds (0 = disabled)
     * @return This builder
     */
    public ConsumerConfigBuilder<K, V> ackTimeoutMs(long ackTimeoutMs) {
        if (ackTimeoutMs < 0) {
            throw new IllegalArgumentException("Ack timeout must be >= 0");
        }
        this.ackTimeoutMs = ackTimeoutMs;
        return this;
    }
    
    /**
     * Set auto-acknowledgment.
     * 
     * @param autoAcknowledge true for auto-ack, false for manual ack
     * @return This builder
     */
    public ConsumerConfigBuilder<K, V> autoAcknowledge(boolean autoAcknowledge) {
        this.autoAcknowledge = autoAcknowledge;
        return this;
    }
    
    /**
     * Set provider-specific properties.
     * 
     * @param providerProperties Properties map
     * @return This builder
     */
    public ConsumerConfigBuilder<K, V> providerProperties(Map<String, Object> providerProperties) {
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
    public ConsumerConfigBuilder<K, V> providerProperty(String key, Object value) {
        if (key != null && value != null) {
            this.providerProperties.put(key, value);
        }
        return this;
    }
    
    /**
     * Build the ConsumerConfig.
     * 
     * @return Immutable ConsumerConfig instance
     * @throws IllegalStateException if required fields are missing
     */
    public ConsumerConfig<K, V> build() {
        if (topic == null || topic.isEmpty()) {
            throw new IllegalStateException("Topic is required");
        }
        if (subscriptionName == null || subscriptionName.isEmpty()) {
            throw new IllegalStateException("Subscription name is required");
        }
        if (subscriptionType == null) {
            throw new IllegalStateException("Subscription type is required");
        }
        if (keySchema == null) {
            throw new IllegalStateException("Key schema is required");
        }
        if (valueSchema == null) {
            throw new IllegalStateException("Value schema is required");
        }
        if (initialPosition == null) {
            throw new IllegalStateException("Initial position is required");
        }
        return new ConsumerConfigImpl<>(
            topic, subscriptionName, subscriptionType, consumerName,
            keySchema, valueSchema, initialPosition, receiverQueueSize,
            ackTimeoutMs, autoAcknowledge, providerProperties
        );
    }
    
    /**
     * Immutable implementation of ConsumerConfig.
     */
    private static class ConsumerConfigImpl<K, V> implements ConsumerConfig<K, V> {
        private final String topic;
        private final String subscriptionName;
        private final SubscriptionType subscriptionType;
        private final String consumerName;
        private final SchemaDefinition keySchema;
        private final SchemaDefinition valueSchema;
        private final InitialPosition initialPosition;
        private final int receiverQueueSize;
        private final long ackTimeoutMs;
        private final boolean autoAcknowledge;
        private final Map<String, Object> providerProperties;
        
        ConsumerConfigImpl(String topic, String subscriptionName,
                          SubscriptionType subscriptionType, String consumerName,
                          SchemaDefinition keySchema, SchemaDefinition valueSchema,
                          InitialPosition initialPosition, int receiverQueueSize,
                          long ackTimeoutMs, boolean autoAcknowledge,
                          Map<String, Object> providerProperties) {
            this.topic = topic;
            this.subscriptionName = subscriptionName;
            this.subscriptionType = subscriptionType;
            this.consumerName = consumerName;
            this.keySchema = keySchema;
            this.valueSchema = valueSchema;
            this.initialPosition = initialPosition;
            this.receiverQueueSize = receiverQueueSize;
            this.ackTimeoutMs = ackTimeoutMs;
            this.autoAcknowledge = autoAcknowledge;
            this.providerProperties = Collections.unmodifiableMap(new HashMap<>(providerProperties));
        }
        
        @Override
        public String getTopic() {
            return topic;
        }
        
        @Override
        public String getSubscriptionName() {
            return subscriptionName;
        }
        
        @Override
        public SubscriptionType getSubscriptionType() {
            return subscriptionType;
        }
        
        @Override
        public Optional<String> getConsumerName() {
            return Optional.ofNullable(consumerName);
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
        public InitialPosition getInitialPosition() {
            return initialPosition;
        }
        
        @Override
        public int getReceiverQueueSize() {
            return receiverQueueSize;
        }
        
        @Override
        public long getAckTimeoutMs() {
            return ackTimeoutMs;
        }
        
        @Override
        public boolean isAutoAcknowledge() {
            return autoAcknowledge;
        }
        
        @Override
        public Map<String, Object> getProviderProperties() {
            return providerProperties;
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ConsumerConfigImpl<?, ?> that = (ConsumerConfigImpl<?, ?>) o;
            return receiverQueueSize == that.receiverQueueSize &&
                   ackTimeoutMs == that.ackTimeoutMs &&
                   autoAcknowledge == that.autoAcknowledge &&
                   Objects.equals(topic, that.topic) &&
                   Objects.equals(subscriptionName, that.subscriptionName) &&
                   subscriptionType == that.subscriptionType &&
                   Objects.equals(consumerName, that.consumerName) &&
                   Objects.equals(keySchema, that.keySchema) &&
                   Objects.equals(valueSchema, that.valueSchema) &&
                   initialPosition == that.initialPosition &&
                   Objects.equals(providerProperties, that.providerProperties);
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(topic, subscriptionName, subscriptionType, consumerName,
                keySchema, valueSchema, initialPosition, receiverQueueSize,
                ackTimeoutMs, autoAcknowledge, providerProperties);
        }
        
        @Override
        public String toString() {
            return "ConsumerConfig{" +
                    "topic='" + topic + '\'' +
                    ", subscriptionName='" + subscriptionName + '\'' +
                    ", subscriptionType=" + subscriptionType +
                    ", consumerName='" + consumerName + '\'' +
                    ", initialPosition=" + initialPosition +
                    ", receiverQueueSize=" + receiverQueueSize +
                    ", ackTimeoutMs=" + ackTimeoutMs +
                    ", autoAcknowledge=" + autoAcknowledge +
                    '}';
        }
    }
}

