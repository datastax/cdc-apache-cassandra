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
package com.datastax.oss.cdc.messaging.stats;

/**
 * Client statistics.
 * Provides metrics about client connection and health.
 */
public interface ClientStats {
    
    /**
     * Get number of active connections.
     * 
     * @return Connection count
     */
    long getConnectionCount();
    
    /**
     * Get number of reconnection attempts.
     * 
     * @return Reconnection count
     */
    long getReconnectionCount();
    
    /**
     * Get number of connection failures.
     * 
     * @return Connection failure count
     */
    long getConnectionFailures();
    
    /**
     * Get number of active producers.
     * 
     * @return Producer count
     */
    long getProducerCount();
    
    /**
     * Get number of active consumers.
     * 
     * @return Consumer count
     */
    long getConsumerCount();
}

