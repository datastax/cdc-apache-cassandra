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
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.schema.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Pulsar implementation of MessageConsumer.
 * Wraps Apache Pulsar consumer for key-value messages.
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
    private final BaseConsumerStats stats = new BaseConsumerStats();
    
    /**
     * Create Pulsar consumer.
     * 
     * @param pulsarClient Pulsar client
     * @param config Consumer configuration
     * @throws PulsarClientException if consumer creation fails
     */
    public PulsarMessageConsumer(PulsarClient pulsarClient, ConsumerConfig<K, V> config) 
            throws PulsarClientException {
        super(config);
        
        log.debug("Creating Pulsar consumer for topic: {}, subscription: {}", 
            config.getTopic(), config.getSubscriptionName());
        
        // Use PulsarConfigMapper to build consumer
        this.pulsarConsumer = PulsarConfigMapper.mapConsumerConfig(pulsarClient, config).subscribe();
        
        markConnected();
        log.info("Pulsar consumer created for topic: {}, subscription: {}", 
            config.getTopic(), config.getSubscriptionName());
    }
    
    @Override
    protected Message<K, V> doReceive(Duration timeout) throws Exception {
        org.apache.pulsar.client.api.Message<KeyValue<K, V>> pulsarMsg =
            pulsarConsumer.receive((int) timeout.toMillis(), TimeUnit.MILLISECONDS);
        
        if (pulsarMsg != null) {
            // Record receive with message size
            stats.recordReceive(pulsarMsg.getData().length);
            return new PulsarMessage<>(pulsarMsg);
        }
        
        return null;
    }
    
    @Override
    protected CompletableFuture<Message<K, V>> doReceiveAsync() {
        return pulsarConsumer.receiveAsync()
            .thenApply(pulsarMsg -> {
                // Record receive with message size
                stats.recordReceive(pulsarMsg.getData().length);
                return (Message<K, V>) new PulsarMessage<>(pulsarMsg);
            });
    }
    
    @Override
    protected void doAcknowledge(Message<K, V> message) throws Exception {
        if (!(message instanceof PulsarMessage)) {
            throw new IllegalArgumentException("Message must be a PulsarMessage");
        }
        
        PulsarMessage<K, V> pulsarMsg = (PulsarMessage<K, V>) message;
        pulsarConsumer.acknowledge(pulsarMsg.getPulsarMessage());
        stats.recordAcknowledgment(0); // Processing latency tracked elsewhere if needed
    }
    
    @Override
    protected CompletableFuture<Void> doAcknowledgeAsync(Message<K, V> message) {
        if (!(message instanceof PulsarMessage)) {
            return CompletableFuture.failedFuture(
                new IllegalArgumentException("Message must be a PulsarMessage"));
        }
        
        PulsarMessage<K, V> pulsarMsg = (PulsarMessage<K, V>) message;
        return pulsarConsumer.acknowledgeAsync(pulsarMsg.getPulsarMessage())
            .thenRun(() -> stats.recordAcknowledgment(0));
    }
    
    @Override
    protected void doNegativeAcknowledge(Message<K, V> message) throws Exception {
        if (!(message instanceof PulsarMessage)) {
            throw new IllegalArgumentException("Message must be a PulsarMessage");
        }
        
        PulsarMessage<K, V> pulsarMsg = (PulsarMessage<K, V>) message;
        pulsarConsumer.negativeAcknowledge(pulsarMsg.getPulsarMessage());
        stats.recordNegativeAcknowledgment();
    }
    
    @Override
    protected void doClose() throws Exception {
        if (pulsarConsumer != null) {
            log.debug("Closing Pulsar consumer for topic: {}, subscription: {}", 
                getTopic(), getSubscription());
            pulsarConsumer.close();
        }
    }
    
    @Override
    public ConsumerStats getStats() {
        return stats;
    }
    
    /**
     * Get underlying Pulsar consumer.
     * For internal use only.
     * 
     * @return Pulsar consumer instance
     */
    Consumer<KeyValue<K, V>> getPulsarConsumer() {
        return pulsarConsumer;
    }
}

// Made with Bob
