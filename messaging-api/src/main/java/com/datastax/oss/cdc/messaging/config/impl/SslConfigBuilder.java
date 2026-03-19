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

import com.datastax.oss.cdc.messaging.config.SslConfig;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Builder for SslConfig.
 * Provides fluent API for constructing immutable SSL/TLS configuration.
 * 
 * <p>Usage:
 * <pre>{@code
 * SslConfig config = SslConfig.builder()
 *     .enabled(true)
 *     .trustStorePath("/path/to/truststore.jks")
 *     .trustStorePassword("password")
 *     .hostnameVerificationEnabled(true)
 *     .build();
 * }</pre>
 */
public class SslConfigBuilder {
    
    private boolean enabled = false;
    private String trustStorePath;
    private String trustStorePassword;
    private String trustStoreType;
    private String keyStorePath;
    private String keyStorePassword;
    private String keyStoreType;
    private String trustedCertificates;
    private String clientCertificate;
    private String clientKey;
    private boolean hostnameVerificationEnabled = true;
    private Set<String> cipherSuites;
    private Set<String> protocols;
    
    private SslConfigBuilder() {
    }
    
    /**
     * Create a new builder.
     * 
     * @return Builder instance
     */
    public static SslConfigBuilder builder() {
        return new SslConfigBuilder();
    }
    
    public SslConfigBuilder enabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }
    
    public SslConfigBuilder trustStorePath(String trustStorePath) {
        this.trustStorePath = trustStorePath;
        return this;
    }
    
    public SslConfigBuilder trustStorePassword(String trustStorePassword) {
        this.trustStorePassword = trustStorePassword;
        return this;
    }
    
    public SslConfigBuilder trustStoreType(String trustStoreType) {
        this.trustStoreType = trustStoreType;
        return this;
    }
    
    public SslConfigBuilder keyStorePath(String keyStorePath) {
        this.keyStorePath = keyStorePath;
        return this;
    }
    
    public SslConfigBuilder keyStorePassword(String keyStorePassword) {
        this.keyStorePassword = keyStorePassword;
        return this;
    }
    
    public SslConfigBuilder keyStoreType(String keyStoreType) {
        this.keyStoreType = keyStoreType;
        return this;
    }
    
    public SslConfigBuilder trustedCertificates(String trustedCertificates) {
        this.trustedCertificates = trustedCertificates;
        return this;
    }
    
    public SslConfigBuilder clientCertificate(String clientCertificate) {
        this.clientCertificate = clientCertificate;
        return this;
    }
    
    public SslConfigBuilder clientKey(String clientKey) {
        this.clientKey = clientKey;
        return this;
    }
    
    public SslConfigBuilder hostnameVerificationEnabled(boolean hostnameVerificationEnabled) {
        this.hostnameVerificationEnabled = hostnameVerificationEnabled;
        return this;
    }
    
    public SslConfigBuilder cipherSuites(Set<String> cipherSuites) {
        this.cipherSuites = cipherSuites != null ? new HashSet<>(cipherSuites) : null;
        return this;
    }
    
    public SslConfigBuilder protocols(Set<String> protocols) {
        this.protocols = protocols != null ? new HashSet<>(protocols) : null;
        return this;
    }
    
    /**
     * Build the SslConfig.
     * 
     * @return Immutable SslConfig instance
     */
    public SslConfig build() {
        return new SslConfigImpl(
            enabled, trustStorePath, trustStorePassword, trustStoreType,
            keyStorePath, keyStorePassword, keyStoreType,
            trustedCertificates, clientCertificate, clientKey,
            hostnameVerificationEnabled, cipherSuites, protocols
        );
    }
    
    /**
     * Immutable implementation of SslConfig.
     */
    private static class SslConfigImpl implements SslConfig {
        private final boolean enabled;
        private final String trustStorePath;
        private final String trustStorePassword;
        private final String trustStoreType;
        private final String keyStorePath;
        private final String keyStorePassword;
        private final String keyStoreType;
        private final String trustedCertificates;
        private final String clientCertificate;
        private final String clientKey;
        private final boolean hostnameVerificationEnabled;
        private final Set<String> cipherSuites;
        private final Set<String> protocols;
        
        SslConfigImpl(boolean enabled, String trustStorePath, String trustStorePassword,
                     String trustStoreType, String keyStorePath, String keyStorePassword,
                     String keyStoreType, String trustedCertificates, String clientCertificate,
                     String clientKey, boolean hostnameVerificationEnabled,
                     Set<String> cipherSuites, Set<String> protocols) {
            this.enabled = enabled;
            this.trustStorePath = trustStorePath;
            this.trustStorePassword = trustStorePassword;
            this.trustStoreType = trustStoreType;
            this.keyStorePath = keyStorePath;
            this.keyStorePassword = keyStorePassword;
            this.keyStoreType = keyStoreType;
            this.trustedCertificates = trustedCertificates;
            this.clientCertificate = clientCertificate;
            this.clientKey = clientKey;
            this.hostnameVerificationEnabled = hostnameVerificationEnabled;
            this.cipherSuites = cipherSuites != null ? 
                Collections.unmodifiableSet(new HashSet<>(cipherSuites)) : null;
            this.protocols = protocols != null ? 
                Collections.unmodifiableSet(new HashSet<>(protocols)) : null;
        }
        
        @Override
        public boolean isEnabled() {
            return enabled;
        }
        
        @Override
        public Optional<String> getTrustStorePath() {
            return Optional.ofNullable(trustStorePath);
        }
        
        @Override
        public Optional<String> getTrustStorePassword() {
            return Optional.ofNullable(trustStorePassword);
        }
        
        @Override
        public Optional<String> getTrustStoreType() {
            return Optional.ofNullable(trustStoreType);
        }
        
        @Override
        public Optional<String> getKeyStorePath() {
            return Optional.ofNullable(keyStorePath);
        }
        
        @Override
        public Optional<String> getKeyStorePassword() {
            return Optional.ofNullable(keyStorePassword);
        }
        
        @Override
        public Optional<String> getKeyStoreType() {
            return Optional.ofNullable(keyStoreType);
        }
        
        @Override
        public Optional<String> getTrustedCertificates() {
            return Optional.ofNullable(trustedCertificates);
        }
        
        @Override
        public Optional<String> getClientCertificate() {
            return Optional.ofNullable(clientCertificate);
        }
        
        @Override
        public Optional<String> getClientKey() {
            return Optional.ofNullable(clientKey);
        }
        
        @Override
        public boolean isHostnameVerificationEnabled() {
            return hostnameVerificationEnabled;
        }
        
        @Override
        public Optional<Set<String>> getCipherSuites() {
            return Optional.ofNullable(cipherSuites);
        }
        
        @Override
        public Optional<Set<String>> getProtocols() {
            return Optional.ofNullable(protocols);
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SslConfigImpl that = (SslConfigImpl) o;
            return enabled == that.enabled &&
                   hostnameVerificationEnabled == that.hostnameVerificationEnabled &&
                   Objects.equals(trustStorePath, that.trustStorePath) &&
                   Objects.equals(trustStoreType, that.trustStoreType) &&
                   Objects.equals(keyStorePath, that.keyStorePath) &&
                   Objects.equals(keyStoreType, that.keyStoreType) &&
                   Objects.equals(cipherSuites, that.cipherSuites) &&
                   Objects.equals(protocols, that.protocols);
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(enabled, trustStorePath, trustStoreType, keyStorePath,
                keyStoreType, hostnameVerificationEnabled, cipherSuites, protocols);
        }
        
        @Override
        public String toString() {
            return "SslConfig{" +
                    "enabled=" + enabled +
                    ", trustStorePath='" + trustStorePath + '\'' +
                    ", keyStorePath='" + keyStorePath + '\'' +
                    ", hostnameVerificationEnabled=" + hostnameVerificationEnabled +
                    '}';
        }
    }
}

