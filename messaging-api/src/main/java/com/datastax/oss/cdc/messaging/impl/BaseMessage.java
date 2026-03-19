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
package com.datastax.oss.cdc.messaging.impl;

import com.datastax.oss.cdc.messaging.Message;
import com.datastax.oss.cdc.messaging.MessageId;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Base implementation of Message.
 * Provides immutable message with key, value, properties, and metadata.
 * 
 * <p>Thread-safe and immutable.
 * 
 * @param <K> Key type
 * @param <V> Value type
 */
public class BaseMessage<K, V> implements Message<K, V> {
    
    private final K key;
    private final V value;
    private final Map<String, String> properties;
    private final MessageId messageId;
    private final String topic;
    private final long eventTime;
    
    /**
     * Create message with builder.
     * Use {@link #builder()} to construct instances.
     */
    private BaseMessage(Builder<K, V> builder) {
        this.key = builder.key;
        this.value = builder.value;
        this.properties = Collections.unmodifiableMap(new HashMap<>(builder.properties));
        this.messageId = Objects.requireNonNull(builder.messageId, "MessageId cannot be null");
        this.topic = Objects.requireNonNull(builder.topic, "Topic cannot be null");
        this.eventTime = builder.eventTime;
    }
    
    @Override
    public K getKey() {
        return key;
    }
    
    @Override
    public V getValue() {
        return value;
    }
    
    @Override
    public Map<String, String> getProperties() {
        return properties;
    }
    
    @Override
    public Optional<String> getProperty(String key) {
        return Optional.ofNullable(properties.get(key));
    }
    
    @Override
    public MessageId getMessageId() {
        return messageId;
    }
    
    @Override
    public String getTopic() {
        return topic;
    }
    
    @Override
    public long getEventTime() {
        return eventTime;
    }
    
    @Override
    public boolean hasKey() {
        return key != null;
    }
    
    @Override
    public String toString() {
        return "BaseMessage{" +
                "messageId=" + messageId +
                ", topic='" + topic + '\'' +
                ", hasKey=" + hasKey() +
                ", hasValue=" + hasValue() +
                ", properties=" + properties.size() +
                ", eventTime=" + eventTime +
                '}';
    }
    
    /**
     * Create a new builder.
     * 
     * @param <K> Key type
     * @param <V> Value type
     * @return Builder instance
     */
    public static <K, V> Builder<K, V> builder() {
        return new Builder<>();
    }
    
    /**
     * Builder for BaseMessage.
     * 
     * @param <K> Key type
     * @param <V> Value type
     */
    public static class Builder<K, V> {
        private K key;
        private V value;
        private Map<String, String> properties = new HashMap<>();
        private MessageId messageId;
        private String topic;
        private long eventTime = System.currentTimeMillis();
        
        private Builder() {
        }
        
        public Builder<K, V> key(K key) {
            this.key = key;
            return this;
        }
        
        public Builder<K, V> value(V value) {
            this.value = value;
            return this;
        }
        
        public Builder<K, V> properties(Map<String, String> properties) {
            if (properties != null) {
                this.properties.putAll(properties);
            }
            return this;
        }
        
        public Builder<K, V> property(String key, String value) {
            if (key != null && value != null) {
                this.properties.put(key, value);
            }
            return this;
        }
        
        public Builder<K, V> messageId(MessageId messageId) {
            this.messageId = messageId;
            return this;
        }
        
        public Builder<K, V> topic(String topic) {
            this.topic = topic;
            return this;
        }
        
        public Builder<K, V> eventTime(long eventTime) {
            this.eventTime = eventTime;
            return this;
        }
        
        /**
         * Build the message.
         * 
         * @return BaseMessage instance
         * @throws IllegalStateException if required fields are missing
         */
        public BaseMessage<K, V> build() {
            if (messageId == null) {
                throw new IllegalStateException("MessageId is required");
            }
            if (topic == null || topic.isEmpty()) {
                throw new IllegalStateException("Topic is required");
            }
            return new BaseMessage<>(this);
        }
    }
}

