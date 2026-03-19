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
 * Configuration for message producer.
 * Contains topic, schema, batching, and routing settings.
 * 
 * @param <K> Key type
 * @param <V> Value type
 */
public interface ProducerConfig<K, V> {
    
    /**
     * Get topic name to publish to.
     * 
     * @return Topic name
     */
    String getTopic();
    
    /**
     * Get producer name for identification.
     * 
     * @return Producer name or empty for auto-generated
     */
    Optional<String> getProducerName();
    
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
     * Get batching configuration.
     * 
     * @return BatchConfig or empty for no batching
     */
    Optional<BatchConfig> getBatchConfig();
    
    /**
     * Get routing configuration.
     * 
     * @return RoutingConfig or empty for default routing
     */
    Optional<RoutingConfig> getRoutingConfig();
    
    /**
     * Get max pending messages before blocking.
     * 
     * @return Max pending messages
     */
    int getMaxPendingMessages();
    
    /**
     * Get send timeout in milliseconds.
     * 
     * @return Timeout in milliseconds
     */
    long getSendTimeoutMs();
    
    /**
     * Check if block on queue full is enabled.
     * 
     * @return true to block, false to fail immediately
     */
    boolean isBlockIfQueueFull();
    
    /**
     * Get compression type.
     * 
     * @return CompressionType or empty for no compression
     */
    Optional<CompressionType> getCompressionType();
    
    /**
     * Get provider-specific properties.
     * 
     * @return Immutable map of properties
     */
    Map<String, Object> getProviderProperties();
}

