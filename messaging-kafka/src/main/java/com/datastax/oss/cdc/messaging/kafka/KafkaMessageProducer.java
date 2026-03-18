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

import com.datastax.oss.cdc.messaging.MessageId;
import com.datastax.oss.cdc.messaging.ProducerException;
import com.datastax.oss.cdc.messaging.config.ProducerConfig;
import com.datastax.oss.cdc.messaging.impl.AbstractMessageProducer;
import com.datastax.oss.cdc.messaging.stats.ProducerStats;
import com.datastax.oss.cdc.messaging.stats.impl.BaseProducerStats;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * Kafka-specific implementation of MessageProducer.
 * Wraps Kafka Producer and provides async send with idempotency support.
 * 
 * <p>Thread-safe.
 */
public class KafkaMessageProducer<K, V> extends AbstractMessageProducer<K, V> {
    
    private static final Logger log = LoggerFactory.getLogger(KafkaMessageProducer.class);
    
    private final KafkaProducer<byte[], byte[]> producer;
    private final String topic;
    private final BaseProducerStats stats;
    private final KafkaSchemaProvider schemaProvider;
    
    /**
     * Create KafkaMessageProducer.
     */
    public KafkaMessageProducer(KafkaProducer<byte[], byte[]> producer,
                                ProducerConfig<K, V> config,
                                KafkaSchemaProvider schemaProvider) {
        super(config);
        this.producer = producer;
        this.topic = config.getTopic();
        this.stats = new BaseProducerStats();
        this.schemaProvider = schemaProvider;
        markConnected();
    }
    
    @Override
    protected CompletableFuture<MessageId> doSendAsync(K key, V value,
                                                       Map<String, String> properties) {
        long startTime = System.nanoTime();
        
        try {
            // Serialize key and value using schema provider
            byte[] keyBytes = schemaProvider.serialize(key, topic, true);
            byte[] valueBytes = value != null ?
                schemaProvider.serialize(value, topic, false) : null;
            
            // Convert properties to Kafka headers
            List<Header> headers = convertPropertiesToHeaders(properties);
            
            // Create Kafka ProducerRecord
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                topic,
                null,  // partition (null = use partitioner)
                null,  // timestamp (null = use current time)
                keyBytes,
                valueBytes,
                headers
            );
            
            // Send asynchronously
            CompletableFuture<MessageId> future = new CompletableFuture<>();
            
            Future<RecordMetadata> kafkaFuture = producer.send(record, (metadata, exception) -> {
                long latencyNanos = System.nanoTime() - startTime;
                long latencyMs = latencyNanos / 1_000_000;
                
                if (exception != null) {
                    stats.recordSendError();
                    future.completeExceptionally(
                        new ProducerException("Failed to send message to Kafka", exception));
                    log.error("Failed to send message to topic {}", topic, exception);
                } else {
                    long bytes = (keyBytes != null ? keyBytes.length : 0) +
                                (valueBytes != null ? valueBytes.length : 0);
                    stats.recordSend(bytes, latencyMs);
                    
                    KafkaMessageId messageId = new KafkaMessageId(metadata);
                    future.complete(messageId);
                    
                    log.debug("Sent message to topic {} partition {} offset {}",
                             metadata.topic(), metadata.partition(), metadata.offset());
                }
            });
            
            return future;
            
        } catch (Exception e) {
            stats.recordSendError();
            log.error("Error preparing message for topic {}", topic, e);
            return CompletableFuture.failedFuture(
                new ProducerException("Error preparing message", e));
        }
    }
    
    
    @Override
    protected void doFlush() throws ProducerException {
        try {
            producer.flush();
            log.debug("Flushed producer for topic {}", topic);
        } catch (Exception e) {
            throw new ProducerException("Failed to flush producer", e);
        }
    }
    
    @Override
    protected void doClose() throws Exception {
        try {
            producer.close();
            log.info("Closed Kafka producer for topic {}", topic);
        } catch (Exception e) {
            log.error("Error closing Kafka producer for topic {}", topic, e);
            throw e;
        }
    }
    
    @Override
    public ProducerStats getStats() {
        return stats;
    }
    
    /**
     * Get the topic name.
     */
    public String getTopic() {
        return topic;
    }
    
    /**
     * Get the underlying Kafka producer.
     */
    public KafkaProducer<byte[], byte[]> getKafkaProducer() {
        return producer;
    }
    
    /**
     * Convert properties map to Kafka headers.
     */
    private List<Header> convertPropertiesToHeaders(Map<String, String> properties) {
        List<Header> headers = new ArrayList<>();
        
        if (properties != null) {
            properties.forEach((key, value) -> {
                if (value != null) {
                    headers.add(new RecordHeader(key, 
                        value.getBytes(StandardCharsets.UTF_8)));
                }
            });
        }
        
        return headers;
    }
}

// Made with Bob
