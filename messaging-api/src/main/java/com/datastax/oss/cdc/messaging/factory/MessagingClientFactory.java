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
package com.datastax.oss.cdc.messaging.factory;

import com.datastax.oss.cdc.messaging.MessagingClient;
import com.datastax.oss.cdc.messaging.MessagingException;
import com.datastax.oss.cdc.messaging.config.ClientConfig;
import com.datastax.oss.cdc.messaging.config.MessagingProvider;
import com.datastax.oss.cdc.messaging.spi.MessagingClientProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating messaging client instances.
 * <p>
 * This factory provides a provider-agnostic way to create messaging clients.
 * It uses the Service Provider Interface (SPI) pattern to discover and
 * instantiate the appropriate provider implementation based on the
 * configuration.
 * </p>
 * <p>
 * Example usage:
 * <pre>
 * ClientConfig config = new ClientConfigBuilder()
 *     .provider(MessagingProvider.PULSAR)
 *     .serviceUrl("pulsar://localhost:6650")
 *     .build();
 *
 * MessagingClient client = MessagingClientFactory.create(config);
 * try {
 *     // Use client
 * } finally {
 *     client.close();
 * }
 * </pre>
 * </p>
 * <p>
 * Thread Safety: This class is thread-safe. Multiple threads can safely
 * call the factory methods concurrently.
 * </p>
 *
 * @see MessagingClient
 * @see ClientConfig
 * @see MessagingClientProvider
 */
public final class MessagingClientFactory {

    private static final Logger log = LoggerFactory.getLogger(MessagingClientFactory.class);

    /**
     * Private constructor to prevent instantiation.
     */
    private MessagingClientFactory() {
        throw new AssertionError("MessagingClientFactory should not be instantiated");
    }

    /**
     * Creates a new messaging client with the given configuration.
     * <p>
     * This method:
     * <ol>
     *   <li>Validates the configuration</li>
     *   <li>Discovers the appropriate provider via SPI</li>
     *   <li>Delegates client creation to the provider</li>
     *   <li>Returns the initialized client</li>
     * </ol>
     * </p>
     *
     * @param config the client configuration
     * @return a new messaging client instance
     * @throws MessagingException if client creation fails
     * @throws IllegalArgumentException if config is null or invalid
     * @throws IllegalStateException if no provider is found for the configured type
     */
    public static MessagingClient create(ClientConfig config) throws MessagingException {
        if (config == null) {
            throw new IllegalArgumentException("ClientConfig cannot be null");
        }

        MessagingProvider providerType = config.getProvider();
        if (providerType == null) {
            throw new IllegalArgumentException("MessagingProvider must be specified in config");
        }

        log.debug("Creating messaging client for provider: {}", providerType);

        try {
            // Get provider from registry
            ProviderRegistry registry = ProviderRegistry.getInstance();
            MessagingClientProvider provider = registry.getProvider(providerType);

            log.debug("Using provider implementation: {}", provider.getClass().getName());

            // Create client via provider
            MessagingClient client = provider.createClient(config);

            log.info("Successfully created messaging client for provider: {}", providerType);
            return client;

        } catch (IllegalStateException e) {
            // No provider found
            log.error("Failed to create messaging client: {}", e.getMessage());
            throw new MessagingException(
                "No provider implementation found for: " + providerType +
                ". Ensure the provider module is on the classpath.", e);
        } catch (MessagingException e) {
            // Provider-specific creation failure
            log.error("Provider failed to create client: {}", e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            // Unexpected error
            log.error("Unexpected error creating messaging client", e);
            throw new MessagingException("Failed to create messaging client", e);
        }
    }

    /**
     * Creates a new messaging client for the specified provider type.
     * <p>
     * This is a convenience method that creates a minimal configuration
     * with the given provider type and service URL.
     * </p>
     *
     * @param provider the messaging provider type
     * @param serviceUrl the service URL
     * @return a new messaging client instance
     * @throws MessagingException if client creation fails
     * @throws IllegalArgumentException if provider or serviceUrl is null
     */
    public static MessagingClient create(MessagingProvider provider, String serviceUrl) 
            throws MessagingException {
        if (provider == null) {
            throw new IllegalArgumentException("MessagingProvider cannot be null");
        }
        if (serviceUrl == null || serviceUrl.trim().isEmpty()) {
            throw new IllegalArgumentException("Service URL cannot be null or empty");
        }

        // Create minimal config - need to use builder from impl package
        ClientConfig config = com.datastax.oss.cdc.messaging.config.impl.ClientConfigBuilder
            .builder()
            .provider(provider)
            .serviceUrl(serviceUrl)
            .build();

        return create(config);
    }

    /**
     * Checks if a provider is available for the given type.
     * <p>
     * This method can be used to verify provider availability before
     * attempting to create a client.
     * </p>
     *
     * @param provider the messaging provider type
     * @return true if a provider is available, false otherwise
     */
    public static boolean isProviderAvailable(MessagingProvider provider) {
        if (provider == null) {
            return false;
        }

        try {
            ProviderRegistry registry = ProviderRegistry.getInstance();
            return registry.hasProvider(provider);
        } catch (Exception e) {
            log.warn("Error checking provider availability: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Returns the number of available providers.
     * <p>
     * This method is primarily for diagnostic and testing purposes.
     * </p>
     *
     * @return the number of registered providers
     */
    public static int getAvailableProviderCount() {
        try {
            ProviderRegistry registry = ProviderRegistry.getInstance();
            return registry.getProviderCount();
        } catch (Exception e) {
            log.warn("Error getting provider count: {}", e.getMessage());
            return 0;
        }
    }

    /**
     * Validates the given configuration.
     * <p>
     * This method performs basic validation checks on the configuration.
     * Provider-specific validation is performed by the provider implementation.
     * </p>
     *
     * @param config the configuration to validate
     * @throws IllegalArgumentException if validation fails
     */
    public static void validate(ClientConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("ClientConfig cannot be null");
        }

        if (config.getProvider() == null) {
            throw new IllegalArgumentException("MessagingProvider must be specified");
        }

        if (config.getServiceUrl() == null || config.getServiceUrl().trim().isEmpty()) {
            throw new IllegalArgumentException("Service URL must be specified");
        }

        // Additional validation can be added here
        log.debug("Configuration validation passed for provider: {}", config.getProvider());
    }
}

// Made with Bob
