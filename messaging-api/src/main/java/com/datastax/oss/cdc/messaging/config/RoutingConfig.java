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
 * Message routing configuration for producer.
 * Controls how messages are routed to partitions.
 */
public interface RoutingConfig {
    
    /**
     * Get routing mode.
     * 
     * @return RoutingMode
     */
    RoutingMode getRoutingMode();
    
    /**
     * Get custom routing class name.
     * Used when routing mode is CUSTOM.
     * 
     * @return Router class name or empty
     */
    String getCustomRouterClassName();
    
    /**
     * Routing mode enumeration.
     */
    enum RoutingMode {
        /**
         * Round-robin routing across partitions.
         */
        ROUND_ROBIN,
        
        /**
         * Hash-based routing using message key.
         */
        KEY_HASH,
        
        /**
         * Single partition routing.
         */
        SINGLE_PARTITION,
        
        /**
         * Custom routing implementation.
         */
        CUSTOM
    }
}

// Made with Bob
