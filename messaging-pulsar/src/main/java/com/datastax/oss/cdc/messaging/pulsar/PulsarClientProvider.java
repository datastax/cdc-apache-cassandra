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
package com.datastax.oss.cdc.messaging.pulsar;

import com.datastax.oss.cdc.messaging.MessagingClient;
import com.datastax.oss.cdc.messaging.MessagingException;
import com.datastax.oss.cdc.messaging.config.ClientConfig;
import com.datastax.oss.cdc.messaging.config.MessagingProvider;
import com.datastax.oss.cdc.messaging.spi.MessagingClientProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service Provider Interface (SPI) implementation for Pulsar.
 * Discovered via Java ServiceLoader mechanism.
 * 
 * <p>Registered in META-INF/services/com.datastax.oss.cdc.messaging.spi.MessagingClientProvider
 * 
 * <p>Thread-safe.
 */
public class PulsarClientProvider implements MessagingClientProvider {
    
    private static final Logger log = LoggerFactory.getLogger(PulsarClientProvider.class);
    
    /**
     * No-arg constructor required for ServiceLoader.
     */
    public PulsarClientProvider() {
        log.debug("PulsarClientProvider instantiated");
    }
    
    @Override
    public MessagingProvider getProvider() {
        return MessagingProvider.PULSAR;
    }
    
    @Override
    public MessagingClient createClient(ClientConfig config) throws MessagingException {
        if (config == null) {
            throw new IllegalArgumentException("ClientConfig cannot be null");
        }
        
        if (config.getProvider() != MessagingProvider.PULSAR) {
            throw new IllegalArgumentException(
                    "Invalid provider: expected PULSAR, got " + config.getProvider());
        }
        
        log.info("Creating Pulsar messaging client");
        
        try {
            PulsarMessagingClient client = new PulsarMessagingClient();
            client.initialize(config);
            return client;
        } catch (Exception e) {
            log.error("Failed to create Pulsar messaging client", e);
            throw new MessagingException("Failed to create Pulsar client", e);
        }
    }
    
    @Override
    public String toString() {
        return "PulsarClientProvider{provider=" + getProvider() + "}";
    }
}

