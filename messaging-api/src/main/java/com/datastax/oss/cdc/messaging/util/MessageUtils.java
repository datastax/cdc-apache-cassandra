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

import com.datastax.oss.cdc.messaging.Message;
import com.datastax.oss.cdc.messaging.impl.BaseMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Message manipulation utilities.
 * Provides helper methods for working with Message instances.
 * 
 * <p>Thread-safe utility class with static methods.
 */
public final class MessageUtils {
    
    private static final Logger log = LoggerFactory.getLogger(MessageUtils.class);
    
    private MessageUtils() {
        // Utility class
    }
    
    /**
     * Create a copy of message with new properties.
     * Original message is not modified.
     * 
     * @param message Original message
     * @param additionalProperties Properties to add/override
     * @param <K> Key type
     * @param <V> Value type
     * @return New message with merged properties
     */
    public static <K, V> Message<K, V> copyWithProperties(
            Message<K, V> message,
            Map<String, String> additionalProperties) {
        
        if (message == null) {
            throw new IllegalArgumentException("Message cannot be null");
        }
        
        Map<String, String> mergedProperties = new HashMap<>(message.getProperties());
        if (additionalProperties != null) {
            mergedProperties.putAll(additionalProperties);
        }
        
        return BaseMessage.<K, V>builder()
            .key(message.getKey())
            .value(message.getValue())
            .properties(mergedProperties)
            .messageId(message.getMessageId())
            .topic(message.getTopic())
            .eventTime(message.getEventTime())
            .build();
    }
    
    /**
     * Create a copy of message with single property added/updated.
     * 
     * @param message Original message
     * @param propertyKey Property key
     * @param propertyValue Property value
     * @param <K> Key type
     * @param <V> Value type
     * @return New message with property
     */
    public static <K, V> Message<K, V> withProperty(
            Message<K, V> message,
            String propertyKey,
            String propertyValue) {
        
        Map<String, String> properties = new HashMap<>();
        properties.put(propertyKey, propertyValue);
        return copyWithProperties(message, properties);
    }
    
    /**
     * Create a copy of message without specified property.
     * 
     * @param message Original message
     * @param propertyKey Property key to remove
     * @param <K> Key type
     * @param <V> Value type
     * @return New message without property
     */
    public static <K, V> Message<K, V> withoutProperty(
            Message<K, V> message,
            String propertyKey) {
        
        if (message == null) {
            throw new IllegalArgumentException("Message cannot be null");
        }
        
        Map<String, String> properties = new HashMap<>(message.getProperties());
        properties.remove(propertyKey);
        
        return BaseMessage.<K, V>builder()
            .key(message.getKey())
            .value(message.getValue())
            .properties(properties)
            .messageId(message.getMessageId())
            .topic(message.getTopic())
            .eventTime(message.getEventTime())
            .build();
    }
    
    /**
     * Check if message is a tombstone (delete marker).
     * Tombstone messages have null value.
     * 
     * @param message Message to check
     * @return true if tombstone
     */
    public static boolean isTombstone(Message<?, ?> message) {
        return message != null && !message.hasValue();
    }
    
    /**
     * Get property value with default.
     * 
     * @param message Message to get property from
     * @param propertyKey Property key
     * @param defaultValue Default value if property not found
     * @return Property value or default
     */
    public static String getPropertyOrDefault(
            Message<?, ?> message,
            String propertyKey,
            String defaultValue) {
        
        if (message == null) {
            return defaultValue;
        }
        
        return message.getProperty(propertyKey).orElse(defaultValue);
    }
    
    /**
     * Check if message has specific property.
     * 
     * @param message Message to check
     * @param propertyKey Property key
     * @return true if property exists
     */
    public static boolean hasProperty(Message<?, ?> message, String propertyKey) {
        return message != null && message.getProperty(propertyKey).isPresent();
    }
    
    /**
     * Get message size estimate in bytes.
     * Includes key, value, and properties.
     * 
     * @param message Message to estimate
     * @return Estimated size in bytes
     */
    public static long estimateSize(Message<?, ?> message) {
        if (message == null) {
            return 0;
        }
        
        long size = 0;
        
        // Estimate key size (rough approximation)
        if (message.hasKey()) {
            size += estimateObjectSize(message.getKey());
        }
        
        // Estimate value size
        if (message.hasValue()) {
            size += estimateObjectSize(message.getValue());
        }
        
        // Estimate properties size
        for (Map.Entry<String, String> entry : message.getProperties().entrySet()) {
            size += entry.getKey().length() * 2; // UTF-16
            size += entry.getValue().length() * 2;
        }
        
        // Message metadata overhead
        size += 100; // Approximate overhead for MessageId, topic, timestamp
        
        return size;
    }
    
    /**
     * Estimate object size (rough approximation).
     * 
     * @param obj Object to estimate
     * @return Estimated size in bytes
     */
    private static long estimateObjectSize(Object obj) {
        if (obj == null) {
            return 0;
        }
        
        if (obj instanceof String) {
            return ((String) obj).length() * 2; // UTF-16
        } else if (obj instanceof byte[]) {
            return ((byte[]) obj).length;
        } else if (obj instanceof Number) {
            return 8; // Assume 8 bytes for numbers
        } else {
            // Default estimate for complex objects
            return 64;
        }
    }
    
    /**
     * Create a tombstone message (null value).
     * Used for delete operations.
     * 
     * @param key Message key
     * @param topic Topic name
     * @param messageId Message ID
     * @param <K> Key type
     * @param <V> Value type
     * @return Tombstone message
     */
    public static <K, V> Message<K, V> createTombstone(
            K key,
            String topic,
            com.datastax.oss.cdc.messaging.MessageId messageId) {
        
        return BaseMessage.<K, V>builder()
            .key(key)
            .value(null)
            .properties(Map.of())
            .messageId(messageId)
            .topic(topic)
            .eventTime(System.currentTimeMillis())
            .build();
    }
    
    /**
     * Log message details for debugging.
     * 
     * @param message Message to log
     * @param prefix Log prefix
     */
    public static void logMessage(Message<?, ?> message, String prefix) {
        if (message == null) {
            log.debug("{}: null message", prefix);
            return;
        }
        
        log.debug("{}: topic={}, hasKey={}, hasValue={}, properties={}, eventTime={}",
            prefix,
            message.getTopic(),
            message.hasKey(),
            message.hasValue(),
            message.getProperties().size(),
            message.getEventTime());
    }
}

