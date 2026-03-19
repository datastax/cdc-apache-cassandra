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

import com.datastax.oss.cdc.messaging.ConsumerException;
import com.datastax.oss.cdc.messaging.Message;
import com.datastax.oss.cdc.messaging.MessageConsumer;
import com.datastax.oss.cdc.messaging.MessagingException;
import com.datastax.oss.cdc.messaging.config.ConsumerConfig;
import com.datastax.oss.cdc.messaging.stats.ConsumerStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Abstract base implementation of MessageConsumer.
 * Provides common functionality for consumer implementations.
 * 
 * <p>Thread-safe for acknowledgment operations, but receive operations
 * should be called from a single thread (as per messaging platform best practices).
 * 
 * <p>Template method pattern:
 * <ul>
 *   <li>{@link #doReceive(Duration)} - Platform-specific receive</li>
 *   <li>{@link #doReceiveAsync()} - Platform-specific async receive</li>
 *   <li>{@link #doAcknowledge(Message)} - Platform-specific acknowledgment</li>
 *   <li>{@link #doAcknowledgeAsync(Message)} - Platform-specific async acknowledgment</li>
 *   <li>{@link #doNegativeAcknowledge(Message)} - Platform-specific negative acknowledgment</li>
 *   <li>{@link #doClose()} - Platform-specific cleanup</li>
 * </ul>
 * 
 * @param <K> Key type
 * @param <V> Value type
 */
public abstract class AbstractMessageConsumer<K, V> implements MessageConsumer<K, V> {
    
    private static final Logger log = LoggerFactory.getLogger(AbstractMessageConsumer.class);
    
    protected final ConsumerConfig<K, V> config;
    protected final AtomicBoolean closed = new AtomicBoolean(false);
    protected final AtomicBoolean connected = new AtomicBoolean(false);
    
    /**
     * Create consumer with configuration.
     * 
     * @param config Consumer configuration
     * @throws IllegalArgumentException if config is null
     */
    protected AbstractMessageConsumer(ConsumerConfig<K, V> config) {
        if (config == null) {
            throw new IllegalArgumentException("ConsumerConfig cannot be null");
        }
        this.config = config;
    }
    
    @Override
    public Message<K, V> receive(Duration timeout) throws MessagingException {
        if (closed.get()) {
            throw new ConsumerException("Consumer is closed");
        }
        if (!connected.get()) {
            throw new ConsumerException("Consumer is not connected");
        }
        
        try {
            return doReceive(timeout);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConsumerException("Receive interrupted", e);
        } catch (Exception e) {
            log.error("Error receiving message", e);
            throw new ConsumerException("Failed to receive message", e);
        }
    }
    
    @Override
    public CompletableFuture<Message<K, V>> receiveAsync() {
        if (closed.get()) {
            return CompletableFuture.failedFuture(
                new ConsumerException("Consumer is closed"));
        }
        if (!connected.get()) {
            return CompletableFuture.failedFuture(
                new ConsumerException("Consumer is not connected"));
        }
        
        try {
            return doReceiveAsync();
        } catch (Exception e) {
            log.error("Error receiving message asynchronously", e);
            return CompletableFuture.failedFuture(
                new ConsumerException("Failed to receive message", e));
        }
    }
    
    @Override
    public void acknowledge(Message<K, V> message) throws MessagingException {
        if (closed.get()) {
            throw new ConsumerException("Consumer is closed");
        }
        if (message == null) {
            throw new IllegalArgumentException("Message cannot be null");
        }
        
        try {
            doAcknowledge(message);
        } catch (Exception e) {
            log.error("Error acknowledging message: {}", message.getMessageId(), e);
            throw new ConsumerException("Failed to acknowledge message", e);
        }
    }
    
    @Override
    public CompletableFuture<Void> acknowledgeAsync(Message<K, V> message) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(
                new ConsumerException("Consumer is closed"));
        }
        if (message == null) {
            return CompletableFuture.failedFuture(
                new IllegalArgumentException("Message cannot be null"));
        }
        
        try {
            return doAcknowledgeAsync(message);
        } catch (Exception e) {
            log.error("Error acknowledging message asynchronously: {}", message.getMessageId(), e);
            return CompletableFuture.failedFuture(
                new ConsumerException("Failed to acknowledge message", e));
        }
    }
    
    @Override
    public void negativeAcknowledge(Message<K, V> message) throws MessagingException {
        if (closed.get()) {
            throw new ConsumerException("Consumer is closed");
        }
        if (message == null) {
            throw new IllegalArgumentException("Message cannot be null");
        }
        
        try {
            doNegativeAcknowledge(message);
        } catch (Exception e) {
            log.error("Error negative acknowledging message: {}", message.getMessageId(), e);
            throw new ConsumerException("Failed to negative acknowledge message", e);
        }
    }
    
    @Override
    public String getSubscription() {
        return config.getSubscriptionName();
    }
    
    @Override
    public String getTopic() {
        return config.getTopic();
    }
    
    @Override
    public boolean isConnected() {
        return connected.get() && !closed.get();
    }
    
    @Override
    public void close() throws MessagingException {
        if (closed.compareAndSet(false, true)) {
            log.info("Closing consumer for topic: {}, subscription: {}", 
                getTopic(), getSubscription());
            try {
                doClose();
                connected.set(false);
                log.info("Consumer closed successfully");
            } catch (Exception e) {
                log.error("Error closing consumer", e);
                throw new ConsumerException("Failed to close consumer", e);
            }
        }
    }
    
    /**
     * Platform-specific receive implementation.
     * Called by {@link #receive(Duration)}.
     * 
     * @param timeout Maximum wait time
     * @return Message or null if timeout
     * @throws Exception if receive fails
     */
    protected abstract Message<K, V> doReceive(Duration timeout) throws Exception;
    
    /**
     * Platform-specific async receive implementation.
     * Called by {@link #receiveAsync()}.
     * 
     * @return CompletableFuture with Message
     */
    protected abstract CompletableFuture<Message<K, V>> doReceiveAsync();
    
    /**
     * Platform-specific acknowledge implementation.
     * Called by {@link #acknowledge(Message)}.
     * 
     * @param message Message to acknowledge
     * @throws Exception if acknowledgment fails
     */
    protected abstract void doAcknowledge(Message<K, V> message) throws Exception;
    
    /**
     * Platform-specific async acknowledge implementation.
     * Called by {@link #acknowledgeAsync(Message)}.
     * 
     * @param message Message to acknowledge
     * @return CompletableFuture for acknowledgment completion
     */
    protected abstract CompletableFuture<Void> doAcknowledgeAsync(Message<K, V> message);
    
    /**
     * Platform-specific negative acknowledge implementation.
     * Called by {@link #negativeAcknowledge(Message)}.
     * 
     * @param message Message to negative acknowledge
     * @throws Exception if negative acknowledgment fails
     */
    protected abstract void doNegativeAcknowledge(Message<K, V> message) throws Exception;
    
    /**
     * Platform-specific close implementation.
     * Called by {@link #close()}.
     * 
     * @throws Exception if close fails
     */
    protected abstract void doClose() throws Exception;
    
    /**
     * Mark consumer as connected.
     * Should be called by subclasses after successful initialization.
     */
    protected void markConnected() {
        connected.set(true);
    }
    
    /**
     * Mark consumer as disconnected.
     * Should be called by subclasses on connection loss.
     */
    protected void markDisconnected() {
        connected.set(false);
    }
}

