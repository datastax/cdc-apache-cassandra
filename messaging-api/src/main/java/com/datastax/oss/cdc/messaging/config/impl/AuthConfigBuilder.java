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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Builder for AuthConfig.
 * Provides fluent API for constructing immutable authentication configuration.
 * 
 * <p>Usage:
 * <pre>{@code
 * AuthConfig config = AuthConfig.builder()
 *     .pluginClassName("org.apache.pulsar.client.impl.auth.AuthenticationToken")
 *     .authParams("token:xxxxx")
 *     .build();
 * }</pre>
 */
public class AuthConfigBuilder {
    
    private String pluginClassName;
    private String authParams;
    private Map<String, String> properties = new HashMap<>();
    
    private AuthConfigBuilder() {
    }
    
    /**
     * Create a new builder.
     * 
     * @return Builder instance
     */
    public static AuthConfigBuilder builder() {
        return new AuthConfigBuilder();
    }
    
    /**
     * Set authentication plugin class name.
     * 
     * @param pluginClassName Plugin class name
     * @return This builder
     */
    public AuthConfigBuilder pluginClassName(String pluginClassName) {
        this.pluginClassName = pluginClassName;
        return this;
    }
    
    /**
     * Set authentication parameters.
     * 
     * @param authParams Authentication parameters
     * @return This builder
     */
    public AuthConfigBuilder authParams(String authParams) {
        this.authParams = authParams;
        return this;
    }
    
    /**
     * Set authentication properties.
     * 
     * @param properties Properties map
     * @return This builder
     */
    public AuthConfigBuilder properties(Map<String, String> properties) {
        if (properties != null) {
            this.properties.putAll(properties);
        }
        return this;
    }
    
    /**
     * Add a single property.
     * 
     * @param key Property key
     * @param value Property value
     * @return This builder
     */
    public AuthConfigBuilder property(String key, String value) {
        if (key != null && value != null) {
            this.properties.put(key, value);
        }
        return this;
    }
    
    /**
     * Build the AuthConfig.
     * 
     * @return Immutable AuthConfig instance
     * @throws IllegalStateException if required fields are missing
     */
    public AuthConfig build() {
        if (pluginClassName == null || pluginClassName.isEmpty()) {
            throw new IllegalStateException("Plugin class name is required");
        }
        if (authParams == null || authParams.isEmpty()) {
            throw new IllegalStateException("Auth params are required");
        }
        return new AuthConfigImpl(pluginClassName, authParams, properties);
    }
    
    /**
     * Immutable implementation of AuthConfig.
     */
    private static class AuthConfigImpl implements AuthConfig {
        private final String pluginClassName;
        private final String authParams;
        private final Map<String, String> properties;
        
        AuthConfigImpl(String pluginClassName, String authParams, Map<String, String> properties) {
            this.pluginClassName = pluginClassName;
            this.authParams = authParams;
            this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
        }
        
        @Override
        public String getPluginClassName() {
            return pluginClassName;
        }
        
        @Override
        public String getAuthParams() {
            return authParams;
        }
        
        @Override
        public Map<String, String> getProperties() {
            return properties;
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AuthConfigImpl that = (AuthConfigImpl) o;
            return Objects.equals(pluginClassName, that.pluginClassName) &&
                   Objects.equals(authParams, that.authParams) &&
                   Objects.equals(properties, that.properties);
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(pluginClassName, authParams, properties);
        }
        
        @Override
        public String toString() {
            return "AuthConfig{" +
                    "pluginClassName='" + pluginClassName + '\'' +
                    ", properties=" + properties.size() +
                    '}';
        }
    }
}

// Made with Bob