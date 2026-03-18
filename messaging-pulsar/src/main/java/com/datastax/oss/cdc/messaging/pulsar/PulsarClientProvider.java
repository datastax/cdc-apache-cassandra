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
import com.datastax.oss.cdc.messaging.config.MessagingProvider;

/**
 * Service Provider Interface (SPI) implementation for Pulsar.
 * Enables discovery and instantiation of Pulsar messaging client.
 * 
 * <p>This class is registered via Java SPI mechanism in META-INF/services.
 */
public class PulsarClientProvider {
    
    /**
     * Get the provider type.
     * 
     * @return MessagingProvider.PULSAR
     */
    public MessagingProvider getProvider() {
        return MessagingProvider.PULSAR;
    }
    
    /**
     * Create a new Pulsar messaging client instance.
     * 
     * @return New PulsarMessagingClient instance
     */
    public MessagingClient createClient() {
        return new PulsarMessagingClient();
    }
    
    /**
     * Get provider name.
     * 
     * @return Provider name
     */
    public String getName() {
        return "Apache Pulsar";
    }
    
    /**
     * Get provider version.
     * 
     * @return Provider version
     */
    public String getVersion() {
        return "3.0.3";
    }
}

// Made with Bob
