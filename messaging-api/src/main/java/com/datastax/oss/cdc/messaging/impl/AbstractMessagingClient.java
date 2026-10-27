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
package com.datastax.oss.cdc.messaging.impl;

import com.datastax.oss.cdc.messaging.ConnectionException;
import com.datastax.oss.cdc.messaging.MessageConsumer;
import com.datastax.oss.cdc.messaging.MessageProducer;
import com.datastax.oss.cdc.messaging.MessagingClient;
import com.datastax.oss.cdc.messaging.MessagingException;
import com.datastax.oss.cdc.messaging.config.ClientConfig;
import com.datastax.oss.cdc.messaging.config.ConsumerConfig;
import com.datastax.oss.cdc.messaging.config.ProducerConfig;
import com.datastax.oss.cdc.messaging.stats.ClientStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Abstract base implementation of MessagingClient.
 * Provides common functionality for client implementations.
 * 
 * <p>Thread-safe. Manages lifecycle and tracks producers/consumers.
 * 
 * <p>Template method pattern:
 * <ul>
 *   <li>{@link #doInitialize(ClientConfig)} - Platform-specific initialization</li>
 *   <li>{@link #doCreateProducer(ProducerConfig)} - Platform-specific producer creation</li>
 *   <li>{@link #doCreateConsumer(ConsumerConfig)} - Platform-specific consumer creation</li>
 *   <li>{@link #doClose()} - Platform-specific cleanup</li>
 * </ul>
 */
public abstract class AbstractMessagingClient implements MessagingClient {
    
    private static final Logger log = LoggerFactory.getLogger(AbstractMessagingClient.class);
    
    protected ClientConfig config;
    protected final AtomicBoolean initialized = new AtomicBoolean(false);
    protected final AtomicBoolean closed = new AtomicBoolean(false);
    protected final AtomicBoolean connected = new AtomicBoolean(false);
    
    // Track created producers and consumers for cleanup
    protected final Map<String, MessageProducer<?, ?>> producers = new ConcurrentHashMap<>();
    protected final Map<String, MessageConsumer<?, ?>> consumers = new ConcurrentHashMap<>();
    
    @Override
    public void initialize(ClientConfig config) throws MessagingException {
        if (config == null) {
            throw new IllegalArgumentException("ClientConfig cannot be null");
        }
        
        if (initialized.get()) {
            log.warn("Client already initialized");
            return;
        }
        
        if (closed.get()) {
            throw new ConnectionException("Client is closed");
        }
        
        log.info("Initializing messaging client for provider: {}", config.getProvider());
        
        try {
            this.config = config;
            doInitialize(config);
            initialized.set(true);
            connected.set(true);
            log.info("Messaging client initialized successfully");
        } catch (Exception e) {
            log.error("Failed to initialize messaging client", e);
            throw new ConnectionException("Failed to initialize client", e);
        }
    }
    
    @Override
    public <K, V> MessageProducer<K, V> createProducer(ProducerConfig<K, V> config) 
            throws MessagingException {
        checkInitialized();
        
        if (config == null) {
            throw new IllegalArgumentException("ProducerConfig cannot be null");
        }
        
        String topic = config.getTopic();
        log.info("Creating producer for topic: {}", topic);
        
        try {
            MessageProducer<K, V> producer = doCreateProducer(config);
            producers.put(topic, producer);
            log.info("Producer created successfully for topic: {}", topic);
            return producer;
        } catch (Exception e) {
            log.error("Failed to create producer for topic: {}", topic, e);
            throw new MessagingException("Failed to create producer", e);
        }
    }
    
    @Override
    public <K, V> MessageConsumer<K, V> createConsumer(ConsumerConfig<K, V> config) 
            throws MessagingException {
        checkInitialized();
        
        if (config == null) {
            throw new IllegalArgumentException("ConsumerConfig cannot be null");
        }
        
        String subscription = config.getSubscriptionName();
        log.info("Creating consumer for topic: {}, subscription: {}", 
            config.getTopic(), subscription);
        
        try {
            MessageConsumer<K, V> consumer = doCreateConsumer(config);
            consumers.put(subscription, consumer);
            log.info("Consumer created successfully for subscription: {}", subscription);
            return consumer;
        } catch (Exception e) {
            log.error("Failed to create consumer for subscription: {}", subscription, e);
            throw new MessagingException("Failed to create consumer", e);
        }
    }
    
    @Override
    public boolean isConnected() {
        return connected.get() && initialized.get() && !closed.get();
    }
    
    @Override
    public void close() throws MessagingException {
        if (closed.compareAndSet(false, true)) {
            log.info("Closing messaging client");
            
            try {
                // Close all producers
                for (Map.Entry<String, MessageProducer<?, ?>> entry : producers.entrySet()) {
                    try {
                        log.debug("Closing producer for topic: {}", entry.getKey());
                        entry.getValue().close();
                    } catch (Exception e) {
                        log.error("Error closing producer for topic: {}", entry.getKey(), e);
                    }
                }
                producers.clear();
                
                // Close all consumers
                for (Map.Entry<String, MessageConsumer<?, ?>> entry : consumers.entrySet()) {
                    try {
                        log.debug("Closing consumer for subscription: {}", entry.getKey());
                        entry.getValue().close();
                    } catch (Exception e) {
                        log.error("Error closing consumer for subscription: {}", entry.getKey(), e);
                    }
                }
                consumers.clear();
                
                // Platform-specific cleanup
                doClose();
                
                connected.set(false);
                initialized.set(false);
                log.info("Messaging client closed successfully");
            } catch (Exception e) {
                log.error("Error closing messaging client", e);
                throw new ConnectionException("Failed to close client", e);
            }
        }
    }
    
    /**
     * Check if client is initialized.
     * 
     * @throws ConnectionException if not initialized
     */
    protected void checkInitialized() throws ConnectionException {
        if (!initialized.get()) {
            throw new ConnectionException("Client not initialized");
        }
        if (closed.get()) {
            throw new ConnectionException("Client is closed");
        }
    }
    
    /**
     * Platform-specific initialization implementation.
     * Called by {@link #initialize(ClientConfig)}.
     * 
     * @param config Client configuration
     * @throws Exception if initialization fails
     */
    protected abstract void doInitialize(ClientConfig config) throws Exception;
    
    /**
     * Platform-specific producer creation implementation.
     * Called by {@link #createProducer(ProducerConfig)}.
     * 
     * @param <K> Key type
     * @param <V> Value type
     * @param config Producer configuration
     * @return MessageProducer instance
     * @throws Exception if creation fails
     */
    protected abstract <K, V> MessageProducer<K, V> doCreateProducer(
        ProducerConfig<K, V> config) throws Exception;
    
    /**
     * Platform-specific consumer creation implementation.
     * Called by {@link #createConsumer(ConsumerConfig)}.
     * 
     * @param <K> Key type
     * @param <V> Value type
     * @param config Consumer configuration
     * @return MessageConsumer instance
     * @throws Exception if creation fails
     */
    protected abstract <K, V> MessageConsumer<K, V> doCreateConsumer(
        ConsumerConfig<K, V> config) throws Exception;
    
    /**
     * Platform-specific close implementation.
     * Called by {@link #close()}.
     * 
     * @throws Exception if close fails
     */
    protected abstract void doClose() throws Exception;
    
    /**
     * Get number of active producers.
     * 
     * @return Producer count
     */
    protected long getProducerCount() {
        return producers.size();
    }
    
    /**
     * Get number of active consumers.
     * 
     * @return Consumer count
     */
    protected long getConsumerCount() {
        return consumers.size();
    }
}

