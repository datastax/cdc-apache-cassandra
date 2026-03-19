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

import java.util.Map;
import java.util.Optional;

/**
 * Configuration for messaging client.
 * Contains connection, authentication, and SSL/TLS settings.
 * 
 * <p>Implementations should be immutable and thread-safe.
 */
public interface ClientConfig {
    
    /**
     * Get messaging provider type.
     * 
     * @return Provider (PULSAR, KAFKA)
     */
    MessagingProvider getProvider();
    
    /**
     * Get service URL or bootstrap servers.
     * Format depends on provider:
     * <ul>
     *   <li>Pulsar: pulsar://host:port or pulsar+ssl://host:port</li>
     *   <li>Kafka: host1:port1,host2:port2</li>
     * </ul>
     * 
     * @return Connection string
     */
    String getServiceUrl();
    
    /**
     * Get authentication configuration.
     * 
     * @return AuthConfig or empty if no authentication
     */
    Optional<AuthConfig> getAuthConfig();
    
    /**
     * Get SSL/TLS configuration.
     * 
     * @return SslConfig or empty if SSL/TLS not enabled
     */
    Optional<SslConfig> getSslConfig();
    
    /**
     * Get provider-specific properties.
     * Allows passing platform-specific configuration that doesn't
     * fit into the common abstraction.
     * 
     * @return Immutable map of properties
     */
    Map<String, Object> getProviderProperties();
    
    /**
     * Get memory limit in bytes for client.
     * Used to limit memory usage for buffering.
     * 
     * @return Memory limit (0 = unlimited)
     */
    long getMemoryLimitBytes();
    
    /**
     * Get operation timeout in milliseconds.
     * 
     * @return Timeout in milliseconds
     */
    long getOperationTimeoutMs();
    
    /**
     * Get connection timeout in milliseconds.
     * 
     * @return Timeout in milliseconds
     */
    long getConnectionTimeoutMs();
}

