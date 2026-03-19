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

import com.datastax.oss.cdc.messaging.ConsumerException;
import com.datastax.oss.cdc.messaging.Message;
import com.datastax.oss.cdc.messaging.config.ConsumerConfig;
import com.datastax.oss.cdc.messaging.impl.AbstractMessageConsumer;
import com.datastax.oss.cdc.messaging.stats.ConsumerStats;
import com.datastax.oss.cdc.messaging.stats.impl.BaseConsumerStats;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Kafka-specific implementation of MessageConsumer.
 * Wraps Kafka Consumer and provides receive/acknowledge operations.
 * 
 * <p>Thread-safe.
 */
public class KafkaMessageConsumer<K, V> extends AbstractMessageConsumer<K, V> {
    
    private static final Logger log = LoggerFactory.getLogger(KafkaMessageConsumer.class);
    
    private final KafkaConsumer<byte[], byte[]> consumer;
    private final String topic;
    private final KafkaOffsetTracker offsetTracker;
    private final BaseConsumerStats stats;
    private final KafkaSchemaProvider schemaProvider;
    private final LinkedBlockingQueue<ConsumerRecord<byte[], byte[]>> messageQueue;
    private final Thread pollingThread;
    private volatile boolean running;
    
    /**
     * Create KafkaMessageConsumer.
     */
    public KafkaMessageConsumer(KafkaConsumer<byte[], byte[]> consumer,
                                ConsumerConfig<K, V> config,
                                KafkaSchemaProvider schemaProvider) {
        super(config);
        this.consumer = consumer;
        this.topic = config.getTopic();
        this.offsetTracker = new KafkaOffsetTracker(consumer);
        this.stats = new BaseConsumerStats();
        this.schemaProvider = schemaProvider;
        this.messageQueue = new LinkedBlockingQueue<>(1000);
        this.running = true;
        
        // Subscribe to topic (Kafka supports pattern subscription via regex)
        consumer.subscribe(java.util.Collections.singletonList(topic));
        log.info("Subscribed to topic: {}", topic);
        
        // Start background polling thread
        this.pollingThread = new Thread(this::pollLoop, "kafka-consumer-poll");
        this.pollingThread.setDaemon(true);
        this.pollingThread.start();
    }
    
    /**
     * Background polling loop that fetches records from Kafka.
     */
    private void pollLoop() {
        log.info("Started Kafka consumer polling thread");
        
        while (running) {
            try {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
                
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    // Track offset for acknowledgment
                    offsetTracker.track(record.topic(), record.partition(), record.offset());
                    
                    // Add to queue for consumption
                    if (!messageQueue.offer(record, 1, TimeUnit.SECONDS)) {
                        log.warn("Message queue full, dropping record from {} partition {} offset {}",
                                record.topic(), record.partition(), record.offset());
                    }
                }
                
            } catch (InterruptedException e) {
                log.info("Polling thread interrupted");
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                if (running) {
                    log.error("Error polling Kafka", e);
                    stats.recordReceiveError();
                }
            }
        }
        
        log.info("Stopped Kafka consumer polling thread");
    }
    
    @Override
    protected Message<K, V> doReceive(Duration timeout) throws ConsumerException {
        long startTime = System.nanoTime();
        
        try {
            ConsumerRecord<byte[], byte[]> record = messageQueue.poll(
                timeout.toMillis(), TimeUnit.MILLISECONDS);
            
            if (record == null) {
                return null;
            }
            
            // Deserialize key and value
            K key = schemaProvider.deserialize(record.key(), record.topic(), true);
            V value = record.value() != null ?
                schemaProvider.deserialize(record.value(), record.topic(), false) : null;
            
            // Create message wrapper with deserialized key/value
            @SuppressWarnings("unchecked")
            Message<K, V> message = (Message<K, V>) new KafkaMessageWrapper<>(record, key, value);
            
            long bytes = (record.key() != null ? record.key().length : 0) +
                        (record.value() != null ? record.value().length : 0);
            stats.recordReceive(bytes);
            
            log.debug("Received message from topic {} partition {} offset {}", 
                     record.topic(), record.partition(), record.offset());
            
            return message;
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConsumerException("Interrupted while receiving message", e);
        } catch (Exception e) {
            stats.recordReceiveError();
            throw new ConsumerException("Failed to receive message", e);
        }
    }
    
    @Override
    protected CompletableFuture<Message<K, V>> doReceiveAsync() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return doReceive(Duration.ofSeconds(30));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
    
    @Override
    protected CompletableFuture<Void> doAcknowledgeAsync(Message<K, V> message) {
        return CompletableFuture.runAsync(() -> {
            try {
                doAcknowledge(message);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
    
    @Override
    protected void doAcknowledge(Message<K, V> message) throws ConsumerException {
        try {
            if (message instanceof KafkaMessageWrapper) {
                @SuppressWarnings("unchecked")
                KafkaMessageWrapper<K, V> kafkaMsg = (KafkaMessageWrapper<K, V>) message;
                ConsumerRecord<byte[], byte[]> record = kafkaMsg.getRecord();
                
                offsetTracker.acknowledge(record.topic(), record.partition(), record.offset());
                long processingLatencyMs = (System.nanoTime() - kafkaMsg.getReceiveTime()) / 1_000_000;
                stats.recordAcknowledgment(processingLatencyMs);
                
                log.debug("Acknowledged message from topic {} partition {} offset {}", 
                         record.topic(), record.partition(), record.offset());
            } else {
                throw new ConsumerException("Message is not a KafkaMessageWrapper");
            }
        } catch (Exception e) {
            throw new ConsumerException("Failed to acknowledge message", e);
        }
    }
    
    @Override
    protected void doNegativeAcknowledge(Message<K, V> message) throws ConsumerException {
        try {
            if (message instanceof KafkaMessageWrapper) {
                @SuppressWarnings("unchecked")
                KafkaMessageWrapper<K, V> kafkaMsg = (KafkaMessageWrapper<K, V>) message;
                ConsumerRecord<byte[], byte[]> record = kafkaMsg.getRecord();
                
                offsetTracker.negativeAcknowledge(record.topic(), record.partition(),
                                                  record.offset());
                stats.recordNegativeAcknowledgment();
                
                log.debug("Negative acknowledged message from topic {} partition {} offset {}", 
                         record.topic(), record.partition(), record.offset());
            } else {
                throw new ConsumerException("Message is not a KafkaMessageWrapper");
            }
        } catch (Exception e) {
            throw new ConsumerException("Failed to negative acknowledge message", e);
        }
    }
    
    @Override
    protected void doClose() throws Exception {
        running = false;
        
        try {
            // Wait for polling thread to stop
            pollingThread.join(5000);
            
            // Close consumer
            consumer.close();
            log.info("Closed Kafka consumer for topic {}", topic);
        } catch (Exception e) {
            log.error("Error closing Kafka consumer", e);
            throw e;
        }
    }
    
    @Override
    public ConsumerStats getStats() {
        return stats;
    }
    
    
    /**
     * Get the offset tracker.
     */
    public KafkaOffsetTracker getOffsetTracker() {
        return offsetTracker;
    }
    
    /**
     * Wrapper class that holds both the raw record and deserialized key/value.
     */
    private static class KafkaMessageWrapper<K, V> extends KafkaMessage<byte[], byte[]> {
        private final K deserializedKey;
        private final V deserializedValue;
        private final long receiveTime;
        
        @SuppressWarnings("unchecked")
        public KafkaMessageWrapper(ConsumerRecord<byte[], byte[]> record, K key, V value) {
            super(record);
            this.deserializedKey = key;
            this.deserializedValue = value;
            this.receiveTime = System.nanoTime();
        }
        
        public long getReceiveTime() {
            return receiveTime;
        }
        
        public K getDeserializedKey() {
            return deserializedKey;
        }
        
        public V getDeserializedValue() {
            return deserializedValue;
        }
        
        public ConsumerRecord<byte[], byte[]> getRecord() {
            return super.getRecord();
        }
    }
}

