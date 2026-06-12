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
package com.datastax.oss.cdc.messaging.config.impl;

import com.datastax.oss.cdc.messaging.config.AuthConfig;
import com.datastax.oss.cdc.messaging.config.ClientConfig;
import com.datastax.oss.cdc.messaging.config.MessagingProvider;
import com.datastax.oss.cdc.messaging.config.SslConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Builder for ClientConfig.
 * Provides fluent API for constructing immutable client configuration.
 * 
 * <p>Usage:
 * <pre>{@code
 * ClientConfig config = ClientConfig.builder()
 *     .provider(MessagingProvider.PULSAR)
 *     .serviceUrl("pulsar://localhost:6650")
 *     .memoryLimitBytes(1024 * 1024 * 100)
 *     .authConfig(authConfig)
 *     .sslConfig(sslConfig)
 *     .build();
 * }</pre>
 */
public class ClientConfigBuilder {
    
    private MessagingProvider provider;
    private String serviceUrl;
    private AuthConfig authConfig;
    private SslConfig sslConfig;
    private Map<String, Object> providerProperties = new HashMap<>();
    private long memoryLimitBytes = 0;
    private long operationTimeoutMs = 30000;
    private long connectionTimeoutMs = 10000;
    
    private ClientConfigBuilder() {
    }
    
    /**
     * Create a new builder.
     * 
     * @return Builder instance
     */
    public static ClientConfigBuilder builder() {
        return new ClientConfigBuilder();
    }
    
    /**
     * Set messaging provider.
     * 
     * @param provider Messaging provider
     * @return This builder
     */
    public ClientConfigBuilder provider(MessagingProvider provider) {
        this.provider = provider;
        return this;
    }
    
    /**
     * Set service URL.
     * 
     * @param serviceUrl Service URL or bootstrap servers
     * @return This builder
     */
    public ClientConfigBuilder serviceUrl(String serviceUrl) {
        this.serviceUrl = serviceUrl;
        return this;
    }
    
    /**
     * Set authentication configuration.
     * 
     * @param authConfig Authentication configuration
     * @return This builder
     */
    public ClientConfigBuilder authConfig(AuthConfig authConfig) {
        this.authConfig = authConfig;
        return this;
    }
    
    /**
     * Set SSL/TLS configuration.
     * 
     * @param sslConfig SSL/TLS configuration
     * @return This builder
     */
    public ClientConfigBuilder sslConfig(SslConfig sslConfig) {
        this.sslConfig = sslConfig;
        return this;
    }
    
    /**
     * Set provider-specific properties.
     * 
     * @param providerProperties Properties map
     * @return This builder
     */
    public ClientConfigBuilder providerProperties(Map<String, Object> providerProperties) {
        if (providerProperties != null) {
            this.providerProperties.putAll(providerProperties);
        }
        return this;
    }
    
    /**
     * Add a single provider property.
     * 
     * @param key Property key
     * @param value Property value
     * @return This builder
     */
    public ClientConfigBuilder providerProperty(String key, Object value) {
        if (key != null && value != null) {
            this.providerProperties.put(key, value);
        }
        return this;
    }
    
    /**
     * Set memory limit in bytes.
     * 
     * @param memoryLimitBytes Memory limit (0 = unlimited)
     * @return This builder
     */
    public ClientConfigBuilder memoryLimitBytes(long memoryLimitBytes) {
        if (memoryLimitBytes < 0) {
            throw new IllegalArgumentException("Memory limit must be >= 0");
        }
        this.memoryLimitBytes = memoryLimitBytes;
        return this;
    }
    
    /**
     * Set operation timeout.
     * 
     * @param operationTimeoutMs Timeout in milliseconds
     * @return This builder
     */
    public ClientConfigBuilder operationTimeoutMs(long operationTimeoutMs) {
        if (operationTimeoutMs <= 0) {
            throw new IllegalArgumentException("Operation timeout must be > 0");
        }
        this.operationTimeoutMs = operationTimeoutMs;
        return this;
    }
    
    /**
     * Set connection timeout.
     * 
     * @param connectionTimeoutMs Timeout in milliseconds
     * @return This builder
     */
    public ClientConfigBuilder connectionTimeoutMs(long connectionTimeoutMs) {
        if (connectionTimeoutMs <= 0) {
            throw new IllegalArgumentException("Connection timeout must be > 0");
        }
        this.connectionTimeoutMs = connectionTimeoutMs;
        return this;
    }
    
    /**
     * Build the ClientConfig.
     * 
     * @return Immutable ClientConfig instance
     * @throws IllegalStateException if required fields are missing
     */
    public ClientConfig build() {
        if (provider == null) {
            throw new IllegalStateException("Provider is required");
        }
        if (serviceUrl == null || serviceUrl.isEmpty()) {
            throw new IllegalStateException("Service URL is required");
        }
        return new ClientConfigImpl(
            provider, serviceUrl, authConfig, sslConfig, providerProperties,
            memoryLimitBytes, operationTimeoutMs, connectionTimeoutMs
        );
    }
    
    /**
     * Immutable implementation of ClientConfig.
     */
    private static class ClientConfigImpl implements ClientConfig {
        private final MessagingProvider provider;
        private final String serviceUrl;
        private final AuthConfig authConfig;
        private final SslConfig sslConfig;
        private final Map<String, Object> providerProperties;
        private final long memoryLimitBytes;
        private final long operationTimeoutMs;
        private final long connectionTimeoutMs;
        
        ClientConfigImpl(MessagingProvider provider, String serviceUrl,
                        AuthConfig authConfig, SslConfig sslConfig,
                        Map<String, Object> providerProperties,
                        long memoryLimitBytes, long operationTimeoutMs,
                        long connectionTimeoutMs) {
            this.provider = provider;
            this.serviceUrl = serviceUrl;
            this.authConfig = authConfig;
            this.sslConfig = sslConfig;
            this.providerProperties = Collections.unmodifiableMap(new HashMap<>(providerProperties));
            this.memoryLimitBytes = memoryLimitBytes;
            this.operationTimeoutMs = operationTimeoutMs;
            this.connectionTimeoutMs = connectionTimeoutMs;
        }
        
        @Override
        public MessagingProvider getProvider() {
            return provider;
        }
        
        @Override
        public String getServiceUrl() {
            return serviceUrl;
        }
        
        @Override
        public Optional<AuthConfig> getAuthConfig() {
            return Optional.ofNullable(authConfig);
        }
        
        @Override
        public Optional<SslConfig> getSslConfig() {
            return Optional.ofNullable(sslConfig);
        }
        
        @Override
        public Map<String, Object> getProviderProperties() {
            return providerProperties;
        }
        
        @Override
        public long getMemoryLimitBytes() {
            return memoryLimitBytes;
        }
        
        @Override
        public long getOperationTimeoutMs() {
            return operationTimeoutMs;
        }
        
        @Override
        public long getConnectionTimeoutMs() {
            return connectionTimeoutMs;
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ClientConfigImpl that = (ClientConfigImpl) o;
            return memoryLimitBytes == that.memoryLimitBytes &&
                   operationTimeoutMs == that.operationTimeoutMs &&
                   connectionTimeoutMs == that.connectionTimeoutMs &&
                   provider == that.provider &&
                   Objects.equals(serviceUrl, that.serviceUrl) &&
                   Objects.equals(authConfig, that.authConfig) &&
                   Objects.equals(sslConfig, that.sslConfig) &&
                   Objects.equals(providerProperties, that.providerProperties);
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(provider, serviceUrl, authConfig, sslConfig,
                providerProperties, memoryLimitBytes, operationTimeoutMs, connectionTimeoutMs);
        }
        
        @Override
        public String toString() {
            return "ClientConfig{" +
                    "provider=" + provider +
                    ", serviceUrl='" + serviceUrl + '\'' +
                    ", hasAuth=" + (authConfig != null) +
                    ", hasSsl=" + (sslConfig != null) +
                    ", memoryLimitBytes=" + memoryLimitBytes +
                    ", operationTimeoutMs=" + operationTimeoutMs +
                    ", connectionTimeoutMs=" + connectionTimeoutMs +
                    '}';
        }
    }
}

