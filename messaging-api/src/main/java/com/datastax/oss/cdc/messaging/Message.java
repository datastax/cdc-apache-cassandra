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
package com.datastax.oss.cdc.messaging;

import java.util.Map;
import java.util.Optional;

/**
 * Abstraction for a message with key, value, and metadata.
 * 
 * <p>Messages are immutable and contain:
 * <ul>
 *   <li>Key (K) - Used for partitioning and ordering</li>
 *   <li>Value (V) - Message payload</li>
 *   <li>Properties - String key-value metadata</li>
 *   <li>MessageId - Unique identifier</li>
 *   <li>Topic - Destination topic name</li>
 *   <li>EventTime - Message timestamp</li>
 * </ul>
 * 
 * @param <K> Key type
 * @param <V> Value type
 */
public interface Message<K, V> {
    
    /**
     * Get message key.
     * Used for partitioning and maintaining ordering.
     * 
     * @return Message key, may be null
     */
    K getKey();
    
    /**
     * Get message value (payload).
     * 
     * @return Message value, may be null for tombstone messages
     */
    V getValue();
    
    /**
     * Get all message properties (metadata).
     * Properties are string key-value pairs used for:
     * <ul>
     *   <li>Routing information</li>
     *   <li>Processing metadata</li>
     *   <li>Application-specific data</li>
     * </ul>
     * 
     * @return Immutable map of properties
     */
    Map<String, String> getProperties();
    
    /**
     * Get a specific property value.
     * 
     * @param key Property key
     * @return Property value or empty if not found
     */
    Optional<String> getProperty(String key);
    
    /**
     * Get unique message identifier.
     * 
     * @return MessageId instance
     */
    MessageId getMessageId();
    
    /**
     * Get topic name where message was published.
     * 
     * @return Topic name
     */
    String getTopic();
    
    /**
     * Get message event timestamp.
     * This is the application-level timestamp, not the broker timestamp.
     * 
     * @return Timestamp in milliseconds since epoch
     */
    long getEventTime();
    
    /**
     * Check if message has a key.
     * 
     * @return true if key is not null
     */
    boolean hasKey();
    
    /**
     * Check if message has a value.
     * Messages without values are tombstones (delete markers).
     * 
     * @return true if value is not null
     */
    default boolean hasValue() {
        return getValue() != null;
    }
}

// Made with Bob
