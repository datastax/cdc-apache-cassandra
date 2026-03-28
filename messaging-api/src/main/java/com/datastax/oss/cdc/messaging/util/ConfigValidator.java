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
package com.datastax.oss.cdc.messaging.util;

import com.datastax.oss.cdc.messaging.config.ConsumerConfig;
import com.datastax.oss.cdc.messaging.config.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Configuration validation utilities.
 * Validates producer and consumer configurations for correctness.
 * 
 * <p>Thread-safe utility class with static methods.
 */
public final class ConfigValidator {
    
    private static final Logger log = LoggerFactory.getLogger(ConfigValidator.class);
    
    // Validation constants
    private static final int MIN_PENDING_MESSAGES = 1;
    private static final int MAX_PENDING_MESSAGES = 1_000_000;
    private static final long MIN_TIMEOUT_MS = 0;
    private static final long MAX_TIMEOUT_MS = 3_600_000; // 1 hour
    private static final int MIN_QUEUE_SIZE = 1;
    private static final int MAX_QUEUE_SIZE = 100_000;
    
    private ConfigValidator() {
        // Utility class
    }
    
    /**
     * Validate producer configuration.
     * 
     * @param config ProducerConfig to validate
     * @param <K> Key type
     * @param <V> Value type
     * @throws IllegalArgumentException if configuration invalid
     */
    public static <K, V> void validateProducerConfig(ProducerConfig<K, V> config) {
        if (config == null) {
            throw new IllegalArgumentException("ProducerConfig cannot be null");
        }
        
        List<String> errors = new ArrayList<>();
        
        // Validate topic
        if (config.getTopic() == null || config.getTopic().trim().isEmpty()) {
            errors.add("Topic name cannot be null or empty");
        }
        
        // Validate schemas
        if (config.getKeySchema() == null) {
            errors.add("Key schema cannot be null");
        }
        if (config.getValueSchema() == null) {
            errors.add("Value schema cannot be null");
        }
        
        // Validate max pending messages
        int maxPending = config.getMaxPendingMessages();
        if (maxPending < MIN_PENDING_MESSAGES || maxPending > MAX_PENDING_MESSAGES) {
            errors.add(String.format("Max pending messages must be between %d and %d, got %d",
                MIN_PENDING_MESSAGES, MAX_PENDING_MESSAGES, maxPending));
        }
        
        // Validate send timeout
        long timeout = config.getSendTimeoutMs();
        if (timeout < MIN_TIMEOUT_MS || timeout > MAX_TIMEOUT_MS) {
            errors.add(String.format("Send timeout must be between %d and %d ms, got %d",
                MIN_TIMEOUT_MS, MAX_TIMEOUT_MS, timeout));
        }
        
        // Validate batch config if present
        config.getBatchConfig().ifPresent(batchConfig -> {
            if (batchConfig.isEnabled()) {
                if (batchConfig.getMaxMessages() <= 0) {
                    errors.add("Batch max messages must be positive");
                }
                if (batchConfig.getMaxBytes() <= 0) {
                    errors.add("Batch max bytes must be positive");
                }
                if (batchConfig.getMaxDelayMs() < 0) {
                    errors.add("Batch max delay cannot be negative");
                }
            }
        });
        
        // Validate routing config if present
        config.getRoutingConfig().ifPresent(routingConfig -> {
            if (routingConfig.getRoutingMode() == null) {
                errors.add("Routing mode cannot be null");
            }
        });
        
        if (!errors.isEmpty()) {
            String message = "ProducerConfig validation failed: " + String.join("; ", errors);
            log.error(message);
            throw new IllegalArgumentException(message);
        }
        
        log.debug("ProducerConfig validation passed for topic: {}", config.getTopic());
    }
    
    /**
     * Validate consumer configuration.
     * 
     * @param config ConsumerConfig to validate
     * @param <K> Key type
     * @param <V> Value type
     * @throws IllegalArgumentException if configuration invalid
     */
    public static <K, V> void validateConsumerConfig(ConsumerConfig<K, V> config) {
        if (config == null) {
            throw new IllegalArgumentException("ConsumerConfig cannot be null");
        }
        
        List<String> errors = new ArrayList<>();
        
        // Validate topic
        if (config.getTopic() == null || config.getTopic().trim().isEmpty()) {
            errors.add("Topic name cannot be null or empty");
        }
        
        // Validate subscription name
        if (config.getSubscriptionName() == null || config.getSubscriptionName().trim().isEmpty()) {
            errors.add("Subscription name cannot be null or empty");
        }
        
        // Validate subscription type
        if (config.getSubscriptionType() == null) {
            errors.add("Subscription type cannot be null");
        }
        
        // Validate schemas
        if (config.getKeySchema() == null) {
            errors.add("Key schema cannot be null");
        }
        if (config.getValueSchema() == null) {
            errors.add("Value schema cannot be null");
        }
        
        // Validate initial position
        if (config.getInitialPosition() == null) {
            errors.add("Initial position cannot be null");
        }
        
        // Validate receiver queue size
        int queueSize = config.getReceiverQueueSize();
        if (queueSize < MIN_QUEUE_SIZE || queueSize > MAX_QUEUE_SIZE) {
            errors.add(String.format("Receiver queue size must be between %d and %d, got %d",
                MIN_QUEUE_SIZE, MAX_QUEUE_SIZE, queueSize));
        }
        
        // Validate ack timeout
        long ackTimeout = config.getAckTimeoutMs();
        if (ackTimeout < MIN_TIMEOUT_MS || ackTimeout > MAX_TIMEOUT_MS) {
            errors.add(String.format("Ack timeout must be between %d and %d ms, got %d",
                MIN_TIMEOUT_MS, MAX_TIMEOUT_MS, ackTimeout));
        }
        
        if (!errors.isEmpty()) {
            String message = "ConsumerConfig validation failed: " + String.join("; ", errors);
            log.error(message);
            throw new IllegalArgumentException(message);
        }
        
        log.debug("ConsumerConfig validation passed for topic: {}, subscription: {}",
            config.getTopic(), config.getSubscriptionName());
    }
    
    /**
     * Check if producer configuration is valid without throwing exception.
     * 
     * @param config ProducerConfig to check
     * @param <K> Key type
     * @param <V> Value type
     * @return true if valid, false otherwise
     */
    public static <K, V> boolean isValidProducerConfig(ProducerConfig<K, V> config) {
        try {
            validateProducerConfig(config);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
    
    /**
     * Check if consumer configuration is valid without throwing exception.
     * 
     * @param config ConsumerConfig to check
     * @param <K> Key type
     * @param <V> Value type
     * @return true if valid, false otherwise
     */
    public static <K, V> boolean isValidConsumerConfig(ConsumerConfig<K, V> config) {
        try {
            validateConsumerConfig(config);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}

