/*
 * Copyright DataStax, Inc.
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
package com.datastax.oss.cdc.messaging.spi;

import com.datastax.oss.cdc.messaging.MessagingClient;
import com.datastax.oss.cdc.messaging.MessagingException;
import com.datastax.oss.cdc.messaging.config.ClientConfig;
import com.datastax.oss.cdc.messaging.config.MessagingProvider;

/**
 * Service Provider Interface (SPI) for messaging client implementations.
 * <p>
 * Implementations of this interface are discovered via Java's ServiceLoader mechanism.
 * Each provider implementation must:
 * <ul>
 *   <li>Implement this interface</li>
 *   <li>Provide a no-arg constructor</li>
 *   <li>Register in META-INF/services/com.datastax.oss.cdc.messaging.spi.MessagingClientProvider</li>
 * </ul>
 * </p>
 * <p>
 * Example provider registration file:
 * <pre>
 * # META-INF/services/com.datastax.oss.cdc.messaging.spi.MessagingClientProvider
 * com.datastax.oss.cdc.messaging.pulsar.PulsarClientProvider
 * com.datastax.oss.cdc.messaging.kafka.KafkaClientProvider
 * </pre>
 * </p>
 * <p>
 * Thread Safety: Implementations must be thread-safe as they may be accessed
 * concurrently by multiple threads.
 * </p>
 *
 * @see MessagingClient
 * @see ClientConfig
 * @see MessagingProvider
 */
public interface MessagingClientProvider {

    /**
     * Returns the messaging provider type supported by this implementation.
     * <p>
     * This method is used by the factory to match the provider with the
     * requested configuration.
     * </p>
     *
     * @return the messaging provider type (e.g., PULSAR, KAFKA)
     */
    MessagingProvider getProvider();

    /**
     * Creates a new messaging client instance with the given configuration.
     * <p>
     * Implementations should:
     * <ul>
     *   <li>Validate the configuration</li>
     *   <li>Initialize platform-specific resources</li>
     *   <li>Return a fully initialized client</li>
     *   <li>Throw MessagingException on any initialization failure</li>
     * </ul>
     * </p>
     * <p>
     * The returned client must be ready to create producers and consumers.
     * </p>
     *
     * @param config the client configuration
     * @return a new messaging client instance
     * @throws MessagingException if client creation fails
     * @throws IllegalArgumentException if config is null or invalid
     */
    MessagingClient createClient(ClientConfig config) throws MessagingException;

    /**
     * Checks if this provider supports the given messaging provider type.
     * <p>
     * This is a convenience method that typically returns:
     * <pre>
     * return getProvider() == provider;
     * </pre>
     * </p>
     *
     * @param provider the messaging provider type to check
     * @return true if this provider supports the given type, false otherwise
     */
    default boolean supports(MessagingProvider provider) {
        return getProvider() == provider;
    }

    /**
     * Returns the provider type as a string identifier.
     * <p>
     * This is used for logging and debugging purposes.
     * Default implementation returns the enum name in lowercase.
     * </p>
     *
     * @return the provider type identifier (e.g., "pulsar", "kafka")
     */
    default String getProviderType() {
        return getProvider().name().toLowerCase();
    }
}

