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

/**
 * Batching configuration for message producer.
 * Controls how messages are batched before sending.
 */
public interface BatchConfig {
    
    /**
     * Check if batching is enabled.
     * 
     * @return true if batching enabled
     */
    boolean isEnabled();
    
    /**
     * Get maximum number of messages in a batch.
     * 
     * @return Max messages per batch
     */
    int getMaxMessages();
    
    /**
     * Get maximum batch size in bytes.
     * 
     * @return Max batch size in bytes
     */
    int getMaxBytes();
    
    /**
     * Get maximum delay before sending batch.
     * Batch is sent when delay expires even if not full.
     * 
     * @return Delay in milliseconds
     */
    long getMaxDelayMs();
    
    /**
     * Check if key-based batching is enabled.
     * Groups messages by key for better ordering.
     * 
     * @return true for key-based batching
     */
    boolean isKeyBasedBatching();
}

// Made with Bob
