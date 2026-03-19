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

import java.util.Optional;
import java.util.Set;

/**
 * SSL/TLS configuration for secure connections.
 */
public interface SslConfig {
    
    /**
     * Check if SSL/TLS is enabled.
     * 
     * @return true if SSL/TLS enabled
     */
    boolean isEnabled();
    
    /**
     * Get trust store path.
     * 
     * @return Trust store file path or empty
     */
    Optional<String> getTrustStorePath();
    
    /**
     * Get trust store password.
     * 
     * @return Trust store password or empty
     */
    Optional<String> getTrustStorePassword();
    
    /**
     * Get trust store type (JKS, PKCS12, etc.).
     * 
     * @return Trust store type or empty (defaults to JKS)
     */
    Optional<String> getTrustStoreType();
    
    /**
     * Get key store path for client certificates.
     * 
     * @return Key store file path or empty
     */
    Optional<String> getKeyStorePath();
    
    /**
     * Get key store password.
     * 
     * @return Key store password or empty
     */
    Optional<String> getKeyStorePassword();
    
    /**
     * Get key store type (JKS, PKCS12, etc.).
     * 
     * @return Key store type or empty (defaults to JKS)
     */
    Optional<String> getKeyStoreType();
    
    /**
     * Get trusted certificates (PEM format).
     * Alternative to trust store.
     * 
     * @return Trusted certificates or empty
     */
    Optional<String> getTrustedCertificates();
    
    /**
     * Get client certificate (PEM format).
     * Alternative to key store.
     * 
     * @return Client certificate or empty
     */
    Optional<String> getClientCertificate();
    
    /**
     * Get client private key (PEM format).
     * Alternative to key store.
     * 
     * @return Client private key or empty
     */
    Optional<String> getClientKey();
    
    /**
     * Check if hostname verification is enabled.
     * 
     * @return true if hostname verification enabled
     */
    boolean isHostnameVerificationEnabled();
    
    /**
     * Get allowed cipher suites.
     * 
     * @return Set of cipher suites or empty for defaults
     */
    Optional<Set<String>> getCipherSuites();
    
    /**
     * Get allowed TLS protocols.
     * 
     * @return Set of protocols or empty for defaults
     */
    Optional<Set<String>> getProtocols();
}

