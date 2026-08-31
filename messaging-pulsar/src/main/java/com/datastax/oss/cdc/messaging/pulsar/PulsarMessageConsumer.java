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
import com.datastax.oss.cdc.messaging.config.ConsumerConfig;
import com.datastax.oss.cdc.messaging.impl.AbstractMessageConsumer;
import com.datastax.oss.cdc.messaging.stats.ConsumerStats;
import com.datastax.oss.cdc.messaging.stats.impl.BaseConsumerStats;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.schema.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Pulsar-specific implementation of MessageConsumer.
 * Wraps Pulsar Consumer and provides messaging abstraction.
 * 
 * <p>Thread-safe for acknowledgment operations.
 * Receive operations should be called from a single thread.
 * 
 * @param <K> Key type
 * @param <V> Value type
 */
public class PulsarMessageConsumer<K, V> extends AbstractMessageConsumer<K, V> {
    
    private static final Logger log = LoggerFactory.getLogger(PulsarMessageConsumer.class);
    
    private final Consumer<KeyValue<K, V>> pulsarConsumer;
    private final BaseConsumerStats stats;
    
    /**
     * Create PulsarMessageConsumer with Pulsar consumer.
     * 
     * @param config Consumer configuration
     * @param pulsarConsumer Pulsar Consumer instance
     * @throws IllegalArgumentException if pulsarConsumer is null
     */
    public PulsarMessageConsumer(ConsumerConfig<K, V> config, Consumer<KeyValue<K, V>> pulsarConsumer) {
        super(config);
        if (pulsarConsumer == null) {
            throw new IllegalArgumentException("Pulsar Consumer cannot be null");
        }
        this.pulsarConsumer = pulsarConsumer;
        this.stats = new BaseConsumerStats();
        markConnected();
        log.info("PulsarMessageConsumer created for topic: {}, subscription: {}", 
                config.getTopic(), config.getSubscriptionName());
    }
    
    @Override
    protected Message<K, V> doReceive(Duration timeout) throws Exception {
        long startTime = System.currentTimeMillis();
        
        try {
            org.apache.pulsar.client.api.Message<KeyValue<K, V>> pulsarMessage = 
                    pulsarConsumer.receive((int) timeout.toMillis(), TimeUnit.MILLISECONDS);
            
            if (pulsarMessage != null) {
                long latency = System.currentTimeMillis() - startTime;
                stats.recordReceive(latency);
                return new PulsarMessage<>(pulsarMessage);
            }
            
            return null;
        } catch (PulsarClientException e) {
            stats.recordReceiveError();
            log.error("Error receiving message from topic: {}", getTopic(), e);
            throw e;
        }
    }
    
    @Override
    protected CompletableFuture<Message<K, V>> doReceiveAsync() {
        long startTime = System.currentTimeMillis();
        
        return pulsarConsumer.receiveAsync()
                .thenApply(pulsarMessage -> {
                    long latency = System.currentTimeMillis() - startTime;
                    stats.recordReceive(latency);
                    return (Message<K, V>) new PulsarMessage<>(pulsarMessage);
                })
                .exceptionally(throwable -> {
                    stats.recordReceiveError();
                    log.error("Error receiving message asynchronously from topic: {}", getTopic(), throwable);
                    throw new RuntimeException("Failed to receive message", throwable);
                });
    }
    
    @Override
    protected void doAcknowledge(Message<K, V> message) throws Exception {
        if (!(message instanceof PulsarMessage)) {
            throw new IllegalArgumentException("Message must be a PulsarMessage");
        }
        
        PulsarMessage<K, V> pulsarMessage = (PulsarMessage<K, V>) message;
        
        try {
            pulsarConsumer.acknowledge(pulsarMessage.getPulsarMessage());
            long processingLatency = 0; // Would need to track receive time to calculate
            stats.recordAcknowledgment(processingLatency);
            log.trace("Acknowledged message: {}", message.getMessageId());
        } catch (PulsarClientException e) {
            stats.recordReceiveError();
            log.error("Error acknowledging message: {}", message.getMessageId(), e);
            throw e;
        }
    }
    
    @Override
    protected CompletableFuture<Void> doAcknowledgeAsync(Message<K, V> message) {
        if (!(message instanceof PulsarMessage)) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("Message must be a PulsarMessage"));
        }
        
        PulsarMessage<K, V> pulsarMessage = (PulsarMessage<K, V>) message;
        
        return pulsarConsumer.acknowledgeAsync(pulsarMessage.getPulsarMessage())
                .thenRun(() -> {
                    long processingLatency = 0; // Would need to track receive time to calculate
                    stats.recordAcknowledgment(processingLatency);
                    log.trace("Acknowledged message asynchronously: {}", message.getMessageId());
                })
                .exceptionally(throwable -> {
                    stats.recordReceiveError();
                    log.error("Error acknowledging message asynchronously: {}", 
                            message.getMessageId(), throwable);
                    throw new RuntimeException("Failed to acknowledge message", throwable);
                });
    }
    
    @Override
    protected void doNegativeAcknowledge(Message<K, V> message) throws Exception {
        if (!(message instanceof PulsarMessage)) {
            throw new IllegalArgumentException("Message must be a PulsarMessage");
        }
        
        PulsarMessage<K, V> pulsarMessage = (PulsarMessage<K, V>) message;
        
        try {
            pulsarConsumer.negativeAcknowledge(pulsarMessage.getPulsarMessage());
            stats.recordNegativeAcknowledgment();
            log.debug("Negative acknowledged message: {}", message.getMessageId());
        } catch (Exception e) {
            log.error("Error negative acknowledging message: {}", message.getMessageId(), e);
            throw e;
        }
    }
    
    @Override
    protected void doClose() throws Exception {
        try {
            pulsarConsumer.close();
            log.info("Closed Pulsar consumer for topic: {}, subscription: {}", 
                    getTopic(), getSubscription());
        } catch (PulsarClientException e) {
            log.error("Error closing Pulsar consumer for topic: {}", getTopic(), e);
            throw e;
        }
    }
    
    @Override
    public ConsumerStats getStats() {
        return stats;
    }
    
    /**
     * Get the underlying Pulsar Consumer.
     * Used for Pulsar-specific operations.
     * 
     * @return Pulsar Consumer instance
     */
    public Consumer<KeyValue<K, V>> getPulsarConsumer() {
        return pulsarConsumer;
    }
    
    @Override
    public boolean isConnected() {
        return super.isConnected() && pulsarConsumer.isConnected();
    }
}

