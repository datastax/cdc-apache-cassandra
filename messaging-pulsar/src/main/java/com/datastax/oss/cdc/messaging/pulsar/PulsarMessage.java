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

import java.util.Map;
import java.util.Optional;

/**
 * Pulsar implementation of Message.
 * Wraps Apache Pulsar message with key-value payload.
 * 
 * <p>Immutable and thread-safe.
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
     * Create message from Pulsar message.
     * 
     * @param pulsarMessage Pulsar message
     */
    public PulsarMessage(org.apache.pulsar.client.api.Message<KeyValue<K, V>> pulsarMessage) {
        if (pulsarMessage == null) {
            throw new IllegalArgumentException("Pulsar message cannot be null");
        }
        
        this.pulsarMessage = pulsarMessage;
        this.messageId = new PulsarMessageId(pulsarMessage.getMessageId());
        
        // Extract key and value from KeyValue
        KeyValue<K, V> keyValue = pulsarMessage.getValue();
        this.key = keyValue != null ? keyValue.getKey() : null;
        this.value = keyValue != null ? keyValue.getValue() : null;
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
        return pulsarMessage.getProperties();
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
    
    @Override
    public boolean hasValue() {
        return value != null;
    }
    
    /**
     * Get underlying Pulsar message.
     * For internal use only.
     * 
     * @return Pulsar message instance
     */
    public org.apache.pulsar.client.api.Message<KeyValue<K, V>> getPulsarMessage() {
        return pulsarMessage;
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
                '}';
    }
}

// Made with Bob
