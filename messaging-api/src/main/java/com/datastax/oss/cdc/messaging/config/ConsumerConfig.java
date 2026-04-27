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
package com.datastax.oss.cdc.messaging.config;

import com.datastax.oss.cdc.messaging.schema.SchemaDefinition;

import java.util.Map;
import java.util.Optional;

/**
 * Configuration for message consumer.
 * Contains subscription, schema, and processing settings.
 * 
 * @param <K> Key type
 * @param <V> Value type
 */
public interface ConsumerConfig<K, V> {
    
    /**
     * Get topic name or pattern to subscribe to.
     * 
     * @return Topic name/pattern
     */
    String getTopic();
    
    /**
     * Get subscription name.
     * Used for tracking consumer position.
     * 
     * @return Subscription name
     */
    String getSubscriptionName();
    
    /**
     * Get subscription type.
     * 
     * @return SubscriptionType
     */
    SubscriptionType getSubscriptionType();
    
    /**
     * Get consumer name for identification.
     * 
     * @return Consumer name or empty for auto-generated
     */
    Optional<String> getConsumerName();
    
    /**
     * Get key schema definition.
     * 
     * @return SchemaDefinition for key
     */
    SchemaDefinition getKeySchema();
    
    /**
     * Get value schema definition.
     * 
     * @return SchemaDefinition for value
     */
    SchemaDefinition getValueSchema();
    
    /**
     * Get initial position for new subscription.
     * 
     * @return InitialPosition (EARLIEST, LATEST)
     */
    InitialPosition getInitialPosition();
    
    /**
     * Get receive queue size.
     * Number of messages to prefetch.
     * 
     * @return Queue size
     */
    int getReceiverQueueSize();
    
    /**
     * Get acknowledgment timeout in milliseconds.
     * Time before unacknowledged message is redelivered.
     * 
     * @return Timeout in milliseconds
     */
    long getAckTimeoutMs();
    
    /**
     * Check if auto-acknowledgment is enabled.
     * 
     * @return true for auto-ack, false for manual ack
     */
    boolean isAutoAcknowledge();
    
    /**
     * Get provider-specific properties.
     * 
     * @return Immutable map of properties
     */
    Map<String, Object> getProviderProperties();
}

