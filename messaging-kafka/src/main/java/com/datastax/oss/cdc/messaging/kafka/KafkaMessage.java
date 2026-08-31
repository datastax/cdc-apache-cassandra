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

import com.datastax.oss.cdc.messaging.Message;
import com.datastax.oss.cdc.messaging.MessageId;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Kafka-specific implementation of Message.
 * Wraps Kafka ConsumerRecord and provides access to key, value, and headers.
 *
 * <p>Immutable and thread-safe.
 */
public class KafkaMessage<K, V> implements Message<K, V> {
    
    private final ConsumerRecord<K, V> record;
    private final Map<String, String> properties;
    private final KafkaMessageId messageId;
    
    /**
     * Create KafkaMessage from ConsumerRecord.
     */
    public KafkaMessage(ConsumerRecord<K, V> record) {
        this.record = record;
        this.properties = Collections.unmodifiableMap(convertHeaders(record));
        this.messageId = new KafkaMessageId(record);
    }
    
    @Override
    public K getKey() {
        return record.key();
    }
    
    @Override
    public V getValue() {
        return record.value();
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
    public long getEventTime() {
        return record.timestamp();
    }
    
    @Override
    public boolean hasKey() {
        return record.key() != null;
    }
    
    /**
     * Get the underlying Kafka ConsumerRecord.
     */
    public ConsumerRecord<K, V> getRecord() {
        return record;
    }
    
    /**
     * Get the topic name.
     */
    public String getTopic() {
        return record.topic();
    }
    
    /**
     * Get the partition number.
     */
    public int getPartition() {
        return record.partition();
    }
    
    /**
     * Get the offset within the partition.
     */
    public long getOffset() {
        return record.offset();
    }
    
    /**
     * Get the timestamp of the record.
     */
    public long getTimestamp() {
        return record.timestamp();
    }
    
    /**
     * Convert Kafka headers to properties map.
     */
    private static Map<String, String> convertHeaders(ConsumerRecord<?, ?> record) {
        Map<String, String> props = new HashMap<>();
        
        for (Header header : record.headers()) {
            String key = header.key();
            byte[] value = header.value();
            
            if (value != null) {
                props.put(key, new String(value, StandardCharsets.UTF_8));
            }
        }
        
        return props;
    }
    
    @Override
    public String toString() {
        return "KafkaMessage{" +
               "topic=" + record.topic() +
               ", partition=" + record.partition() +
               ", offset=" + record.offset() +
               ", timestamp=" + record.timestamp() +
               ", key=" + record.key() +
               ", value=" + record.value() +
               '}';
    }
}

