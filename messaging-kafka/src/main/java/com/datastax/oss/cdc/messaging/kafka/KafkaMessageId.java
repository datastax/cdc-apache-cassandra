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

import com.datastax.oss.cdc.messaging.impl.BaseMessageId;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Kafka-specific implementation of MessageId.
 * Wraps Kafka's topic-partition-offset identifier.
 * 
 * <p>Immutable and thread-safe.
 */
public class KafkaMessageId extends BaseMessageId {
    
    private final String topic;
    private final int partition;
    private final long offset;
    
    /**
     * Create KafkaMessageId from RecordMetadata.
     */
    public KafkaMessageId(RecordMetadata metadata) {
        this(metadata.topic(), metadata.partition(), metadata.offset());
    }
    
    /**
     * Create KafkaMessageId from ConsumerRecord.
     */
    public KafkaMessageId(ConsumerRecord<?, ?> record) {
        this(record.topic(), record.partition(), record.offset());
    }
    
    /**
     * Create KafkaMessageId from components.
     */
    public KafkaMessageId(String topic, int partition, long offset) {
        super(createIdBytes(topic, partition, offset));
        this.topic = Objects.requireNonNull(topic, "topic cannot be null");
        this.partition = partition;
        this.offset = offset;
    }
    
    /**
     * Create byte array representation for BaseMessageId constructor.
     */
    private static byte[] createIdBytes(String topic, int partition, long offset) {
        byte[] topicBytes = topic.getBytes();
        ByteBuffer buffer = ByteBuffer.allocate(4 + topicBytes.length + 4 + 8);
        buffer.putInt(topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partition);
        buffer.putLong(offset);
        return buffer.array();
    }
    
    @Override
    public byte[] toByteArray() {
        // Encode as: topic_length(4) + topic_bytes + partition(4) + offset(8)
        byte[] topicBytes = topic.getBytes();
        ByteBuffer buffer = ByteBuffer.allocate(4 + topicBytes.length + 4 + 8);
        buffer.putInt(topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partition);
        buffer.putLong(offset);
        return buffer.array();
    }
    
    /**
     * Get the topic name.
     */
    public String getTopic() {
        return topic;
    }
    
    /**
     * Get the partition number.
     */
    public int getPartition() {
        return partition;
    }
    
    /**
     * Get the offset within the partition.
     */
    public long getOffset() {
        return offset;
    }
    
    @Override
    public String toString() {
        return topic + "-" + partition + "-" + offset;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof KafkaMessageId)) return false;
        KafkaMessageId that = (KafkaMessageId) o;
        return partition == that.partition &&
               offset == that.offset &&
               topic.equals(that.topic);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(topic, partition, offset);
    }
}

