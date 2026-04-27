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

import com.datastax.oss.cdc.messaging.Message;
import com.datastax.oss.cdc.messaging.MessageId;
import org.apache.pulsar.common.schema.KeyValue;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * Pulsar-specific implementation of Message.
 * Wraps Pulsar's native Message and provides access to it for Pulsar-specific operations.
 * 
 * <p>Thread-safe and immutable.
 * 
 * @param <K> Key type
 * @param <V> Value type
 */
public class PulsarMessage<K, V> implements Message<K, V> {
    
    private final org.apache.pulsar.client.api.Message<KeyValue<K, V>> pulsarMessage;
    private final PulsarMessageId messageId;
    private final K key;
    private final V value;
    
    /**
     * Create PulsarMessage from Pulsar's native Message.
     * 
     * @param pulsarMessage Pulsar Message instance with KeyValue payload
     * @throws IllegalArgumentException if pulsarMessage is null
     */
    public PulsarMessage(org.apache.pulsar.client.api.Message<KeyValue<K, V>> pulsarMessage) {
        if (pulsarMessage == null) {
            throw new IllegalArgumentException("Pulsar Message cannot be null");
        }
        this.pulsarMessage = pulsarMessage;
        this.messageId = new PulsarMessageId(pulsarMessage.getMessageId());
        
        // Extract key and value from KeyValue payload
        KeyValue<K, V> keyValue = pulsarMessage.getValue();
        if (keyValue != null) {
            this.key = keyValue.getKey();
            this.value = keyValue.getValue();
        } else {
            this.key = null;
            this.value = null;
        }
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
        Map<String, String> props = pulsarMessage.getProperties();
        return props != null ? Collections.unmodifiableMap(props) : Collections.emptyMap();
    }
    
    @Override
    public Optional<String> getProperty(String key) {
        return Optional.ofNullable(pulsarMessage.getProperty(key));
    }
    
    @Override
    public MessageId getMessageId() {
        return messageId;
    }
    
    @Override
    public String getTopic() {
        return pulsarMessage.getTopicName();
    }
    
    @Override
    public long getEventTime() {
        return pulsarMessage.getEventTime();
    }
    
    @Override
    public boolean hasKey() {
        return key != null;
    }
    
    /**
     * Get the underlying Pulsar Message.
     * Used for Pulsar-specific operations like acknowledgment.
     * 
     * @return Pulsar Message instance
     */
    public org.apache.pulsar.client.api.Message<KeyValue<K, V>> getPulsarMessage() {
        return pulsarMessage;
    }
    
    /**
     * Get publish timestamp from Pulsar message.
     * 
     * @return Publish timestamp in milliseconds since epoch
     */
    public long getPublishTime() {
        return pulsarMessage.getPublishTime();
    }
    
    @Override
    public String toString() {
        return "PulsarMessage{" +
                "messageId=" + messageId +
                ", topic='" + getTopic() + '\'' +
                ", hasKey=" + hasKey() +
                ", hasValue=" + hasValue() +
                ", properties=" + getProperties().size() +
                ", eventTime=" + getEventTime() +
                ", publishTime=" + getPublishTime() +
                '}';
    }
}

