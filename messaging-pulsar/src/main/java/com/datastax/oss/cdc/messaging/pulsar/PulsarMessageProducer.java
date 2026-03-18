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
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.common.schema.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Pulsar implementation of MessageProducer.
 * Wraps Apache Pulsar producer for key-value messages.
 * 
 * <p>Thread-safe. Supports batching, compression, and routing.
 * 
 * @param <K> Key type
 * @param <V> Value type
 */
public class PulsarMessageProducer<K, V> extends AbstractMessageProducer<K, V> {
    
    private static final Logger log = LoggerFactory.getLogger(PulsarMessageProducer.class);
    
    private final Producer<KeyValue<K, V>> pulsarProducer;
    private final BaseProducerStats stats = new BaseProducerStats();
    
    /**
     * Create Pulsar producer.
     * 
     * @param pulsarClient Pulsar client
     * @param config Producer configuration
     * @throws PulsarClientException if producer creation fails
     */
    public PulsarMessageProducer(PulsarClient pulsarClient, ProducerConfig<K, V> config) 
            throws PulsarClientException {
        super(config);
        
        log.debug("Creating Pulsar producer for topic: {}", config.getTopic());
        
        // Use PulsarConfigMapper to build producer
        this.pulsarProducer = PulsarConfigMapper.mapProducerConfig(pulsarClient, config).create();
        
        markConnected();
        log.info("Pulsar producer created for topic: {}", config.getTopic());
    }
    
    @Override
    protected CompletableFuture<MessageId> doSendAsync(K key, V value, Map<String, String> properties) {
        long startTime = System.nanoTime();
        
        // Build Pulsar message
        TypedMessageBuilder<KeyValue<K, V>> builder = pulsarProducer.newMessage()
            .value(new KeyValue<>(key, value));
        
        // Add properties
        if (properties != null && !properties.isEmpty()) {
            properties.forEach(builder::property);
        }
        
        // Send asynchronously
        return builder.sendAsync()
            .thenApply(msgId -> {
                long latencyNanos = System.nanoTime() - startTime;
                long latencyMs = latencyNanos / 1_000_000;
                // Estimate message size (simplified)
                stats.recordSend(100, latencyMs); // Using 100 as placeholder for bytes
                return (MessageId) new PulsarMessageId(msgId);
            })
            .exceptionally(ex -> {
                stats.recordSendError();
                log.error("Failed to send message to topic: {}", getTopic(), ex);
                throw new RuntimeException("Send failed", ex);
            });
    }
    
    @Override
    protected void doFlush() throws Exception {
        pulsarProducer.flush();
    }
    
    @Override
    protected void doClose() throws Exception {
        if (pulsarProducer != null) {
            log.debug("Closing Pulsar producer for topic: {}", getTopic());
            pulsarProducer.close();
        }
    }
    
    @Override
    public ProducerStats getStats() {
        // Update stats from Pulsar producer
        org.apache.pulsar.client.api.ProducerStats pulsarStats = pulsarProducer.getStats();
        stats.setPendingMessages(pulsarStats.getNumMsgsSent() - pulsarStats.getNumAcksReceived());
        
        return stats;
    }
    
    /**
     * Get underlying Pulsar producer.
     * For internal use only.
     * 
     * @return Pulsar producer instance
     */
    Producer<KeyValue<K, V>> getPulsarProducer() {
        return pulsarProducer;
    }
}

// Made with Bob
