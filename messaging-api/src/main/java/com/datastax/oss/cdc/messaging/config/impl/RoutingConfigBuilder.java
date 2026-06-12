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

import com.datastax.oss.cdc.messaging.config.RoutingConfig;

import java.util.Objects;

/**
 * Builder for RoutingConfig.
 * Provides fluent API for constructing immutable routing configuration.
 * 
 * <p>Usage:
 * <pre>{@code
 * RoutingConfig config = RoutingConfig.builder()
 *     .routingMode(RoutingMode.KEY_HASH)
 *     .build();
 * }</pre>
 */
public class RoutingConfigBuilder {
    
    private RoutingConfig.RoutingMode routingMode = RoutingConfig.RoutingMode.KEY_HASH;
    private String customRouterClassName;
    
    private RoutingConfigBuilder() {
    }
    
    /**
     * Create a new builder.
     * 
     * @return Builder instance
     */
    public static RoutingConfigBuilder builder() {
        return new RoutingConfigBuilder();
    }
    
    /**
     * Set routing mode.
     * 
     * @param routingMode Routing mode
     * @return This builder
     */
    public RoutingConfigBuilder routingMode(RoutingConfig.RoutingMode routingMode) {
        this.routingMode = routingMode;
        return this;
    }
    
    /**
     * Set custom router class name.
     * Required when routing mode is CUSTOM.
     * 
     * @param customRouterClassName Router class name
     * @return This builder
     */
    public RoutingConfigBuilder customRouterClassName(String customRouterClassName) {
        this.customRouterClassName = customRouterClassName;
        return this;
    }
    
    /**
     * Build the RoutingConfig.
     * 
     * @return Immutable RoutingConfig instance
     * @throws IllegalStateException if validation fails
     */
    public RoutingConfig build() {
        if (routingMode == null) {
            throw new IllegalStateException("Routing mode is required");
        }
        if (routingMode == RoutingConfig.RoutingMode.CUSTOM && 
            (customRouterClassName == null || customRouterClassName.isEmpty())) {
            throw new IllegalStateException(
                "Custom router class name is required for CUSTOM routing mode");
        }
        return new RoutingConfigImpl(routingMode, customRouterClassName);
    }
    
    /**
     * Immutable implementation of RoutingConfig.
     */
    private static class RoutingConfigImpl implements RoutingConfig {
        private final RoutingMode routingMode;
        private final String customRouterClassName;
        
        RoutingConfigImpl(RoutingMode routingMode, String customRouterClassName) {
            this.routingMode = routingMode;
            this.customRouterClassName = customRouterClassName;
        }
        
        @Override
        public RoutingMode getRoutingMode() {
            return routingMode;
        }
        
        @Override
        public String getCustomRouterClassName() {
            return customRouterClassName;
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RoutingConfigImpl that = (RoutingConfigImpl) o;
            return routingMode == that.routingMode &&
                   Objects.equals(customRouterClassName, that.customRouterClassName);
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(routingMode, customRouterClassName);
        }
        
        @Override
        public String toString() {
            return "RoutingConfig{" +
                    "routingMode=" + routingMode +
                    ", customRouterClassName='" + customRouterClassName + '\'' +
                    '}';
        }
    }
}

