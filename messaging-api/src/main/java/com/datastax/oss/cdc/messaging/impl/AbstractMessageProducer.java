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

import com.datastax.oss.cdc.messaging.MessageId;
import com.datastax.oss.cdc.messaging.MessageProducer;
import com.datastax.oss.cdc.messaging.MessagingException;
import com.datastax.oss.cdc.messaging.ProducerException;
import com.datastax.oss.cdc.messaging.config.ProducerConfig;
import com.datastax.oss.cdc.messaging.stats.ProducerStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Abstract base implementation of MessageProducer.
 * Provides common functionality for producer implementations.
 * 
 * <p>Thread-safe. Subclasses must implement platform-specific send operations.
 * 
 * <p>Template method pattern:
 * <ul>
 *   <li>{@link #doSendAsync(Object, Object, Map)} - Platform-specific async send</li>
 *   <li>{@link #doFlush()} - Platform-specific flush</li>
 *   <li>{@link #doClose()} - Platform-specific cleanup</li>
 * </ul>
 * 
 * @param <K> Key type
 * @param <V> Value type
 */
public abstract class AbstractMessageProducer<K, V> implements MessageProducer<K, V> {
    
    private static final Logger log = LoggerFactory.getLogger(AbstractMessageProducer.class);
    
    protected final ProducerConfig<K, V> config;
    protected final AtomicBoolean closed = new AtomicBoolean(false);
    protected final AtomicBoolean connected = new AtomicBoolean(false);
    
    /**
     * Create producer with configuration.
     * 
     * @param config Producer configuration
     * @throws IllegalArgumentException if config is null
     */
    protected AbstractMessageProducer(ProducerConfig<K, V> config) {
        if (config == null) {
            throw new IllegalArgumentException("ProducerConfig cannot be null");
        }
        this.config = config;
    }
    
    @Override
    public CompletableFuture<MessageId> sendAsync(K key, V value, Map<String, String> properties) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(
                new ProducerException("Producer is closed"));
        }
        if (!connected.get()) {
            return CompletableFuture.failedFuture(
                new ProducerException("Producer is not connected"));
        }
        
        try {
            return doSendAsync(key, value, properties);
        } catch (Exception e) {
            log.error("Error sending message asynchronously", e);
            return CompletableFuture.failedFuture(
                new ProducerException("Failed to send message", e));
        }
    }
    
    @Override
    public MessageId send(K key, V value, Map<String, String> properties) throws MessagingException {
        CompletableFuture<MessageId> future = sendAsync(key, value, properties);
        
        try {
            long timeoutMs = config.getSendTimeoutMs();
            if (timeoutMs > 0) {
                return future.get(timeoutMs, TimeUnit.MILLISECONDS);
            } else {
                return future.get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ProducerException("Send interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof MessagingException) {
                throw (MessagingException) cause;
            }
            throw new ProducerException("Send failed", cause);
        } catch (TimeoutException e) {
            throw new ProducerException("Send timeout after " + config.getSendTimeoutMs() + "ms", e);
        }
    }
    
    @Override
    public void flush() throws MessagingException {
        if (closed.get()) {
            throw new ProducerException("Producer is closed");
        }
        
        try {
            doFlush();
        } catch (Exception e) {
            log.error("Error flushing producer", e);
            throw new ProducerException("Failed to flush producer", e);
        }
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
            log.info("Closing producer for topic: {}", getTopic());
            try {
                // Flush pending messages before closing
                if (connected.get()) {
                    doFlush();
                }
                doClose();
                connected.set(false);
                log.info("Producer closed successfully");
            } catch (Exception e) {
                log.error("Error closing producer", e);
                throw new ProducerException("Failed to close producer", e);
            }
        }
    }
    
    /**
     * Platform-specific async send implementation.
     * Called by {@link #sendAsync(Object, Object, Map)}.
     * 
     * @param key Message key
     * @param value Message value
     * @param properties Message properties
     * @return CompletableFuture with MessageId
     */
    protected abstract CompletableFuture<MessageId> doSendAsync(
        K key, V value, Map<String, String> properties);
    
    /**
     * Platform-specific flush implementation.
     * Called by {@link #flush()}.
     * 
     * @throws Exception if flush fails
     */
    protected abstract void doFlush() throws Exception;
    
    /**
     * Platform-specific close implementation.
     * Called by {@link #close()}.
     * 
     * @throws Exception if close fails
     */
    protected abstract void doClose() throws Exception;
    
    /**
     * Mark producer as connected.
     * Should be called by subclasses after successful initialization.
     */
    protected void markConnected() {
        connected.set(true);
    }
    
    /**
     * Mark producer as disconnected.
     * Should be called by subclasses on connection loss.
     */
    protected void markDisconnected() {
        connected.set(false);
    }
}

