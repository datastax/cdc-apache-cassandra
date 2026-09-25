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

import com.datastax.oss.cdc.messaging.MessageId;
import com.datastax.oss.cdc.messaging.config.ProducerConfig;
import com.datastax.oss.cdc.messaging.impl.AbstractMessageProducer;
import com.datastax.oss.cdc.messaging.stats.ProducerStats;
import com.datastax.oss.cdc.messaging.stats.impl.BaseProducerStats;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.common.schema.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Pulsar-specific implementation of MessageProducer.
 * Wraps Pulsar Producer and provides messaging abstraction.
 * 
 * <p>Thread-safe.
 * 
 * @param <K> Key type
 * @param <V> Value type
 */
public class PulsarMessageProducer<K, V> extends AbstractMessageProducer<K, V> {
    
    private static final Logger log = LoggerFactory.getLogger(PulsarMessageProducer.class);
    
    private final Producer<KeyValue<K, V>> pulsarProducer;
    private final BaseProducerStats stats;
    
    /**
     * Create PulsarMessageProducer with Pulsar producer.
     * 
     * @param config Producer configuration
     * @param pulsarProducer Pulsar Producer instance
     * @throws IllegalArgumentException if pulsarProducer is null
     */
    public PulsarMessageProducer(ProducerConfig<K, V> config, Producer<KeyValue<K, V>> pulsarProducer) {
        super(config);
        if (pulsarProducer == null) {
            throw new IllegalArgumentException("Pulsar Producer cannot be null");
        }
        this.pulsarProducer = pulsarProducer;
        this.stats = new BaseProducerStats();
        markConnected();
        log.info("PulsarMessageProducer created for topic: {}", config.getTopic());
    }
    
    @Override
    protected CompletableFuture<MessageId> doSendAsync(K key, V value, Map<String, String> properties) {
        long startTime = System.currentTimeMillis();
        
        try {
            // Create KeyValue payload
            KeyValue<K, V> keyValue = new KeyValue<>(key, value);
            
            // Build message
            TypedMessageBuilder<KeyValue<K, V>> builder = pulsarProducer.newMessage()
                    .value(keyValue);
            
            // Add properties if provided
            if (properties != null && !properties.isEmpty()) {
                properties.forEach(builder::property);
            }
            
            // Send asynchronously
            CompletableFuture<org.apache.pulsar.client.api.MessageId> pulsarFuture = builder.sendAsync();
            
            // Convert Pulsar MessageId to our MessageId
            return pulsarFuture.thenApply(pulsarMessageId -> {
                long latency = System.currentTimeMillis() - startTime;
                // Estimate message size (key + value + properties)
                long estimatedBytes = 100; // Rough estimate
                stats.recordSend(estimatedBytes, latency);
                return (MessageId) new PulsarMessageId(pulsarMessageId);
            }).exceptionally(throwable -> {
                stats.recordSendError();
                log.error("Failed to send message to topic: {}", getTopic(), throwable);
                throw new RuntimeException("Failed to send message", throwable);
            });
            
        } catch (Exception e) {
            stats.recordSendError();
            log.error("Error creating message for topic: {}", getTopic(), e);
            return CompletableFuture.failedFuture(e);
        }
    }
    
    @Override
    protected void doFlush() throws Exception {
        try {
            pulsarProducer.flush();
            log.debug("Flushed producer for topic: {}", getTopic());
        } catch (PulsarClientException e) {
            log.error("Error flushing producer for topic: {}", getTopic(), e);
            throw e;
        }
    }
    
    @Override
    protected void doClose() throws Exception {
        try {
            pulsarProducer.close();
            log.info("Closed Pulsar producer for topic: {}", getTopic());
        } catch (PulsarClientException e) {
            log.error("Error closing Pulsar producer for topic: {}", getTopic(), e);
            throw e;
        }
    }
    
    @Override
    public ProducerStats getStats() {
        return stats;
    }
    
    /**
     * Get the underlying Pulsar Producer.
     * Used for Pulsar-specific operations.
     * 
     * @return Pulsar Producer instance
     */
    public Producer<KeyValue<K, V>> getPulsarProducer() {
        return pulsarProducer;
    }
    
    @Override
    public boolean isConnected() {
        return super.isConnected() && pulsarProducer.isConnected();
    }
}

