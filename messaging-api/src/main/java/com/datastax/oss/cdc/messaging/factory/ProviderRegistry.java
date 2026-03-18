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

import com.datastax.oss.cdc.messaging.config.MessagingProvider;
import com.datastax.oss.cdc.messaging.spi.MessagingClientProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Registry for managing messaging client provider implementations.
 * <p>
 * This class uses Java's ServiceLoader mechanism to discover and cache
 * provider implementations at runtime. Providers are loaded lazily on
 * first access and cached for subsequent use.
 * </p>
 * <p>
 * Thread Safety: This class is thread-safe. Provider discovery and
 * registration are protected by a read-write lock.
 * </p>
 * <p>
 * Example usage:
 * <pre>
 * ProviderRegistry registry = ProviderRegistry.getInstance();
 * MessagingClientProvider provider = registry.getProvider(MessagingProvider.PULSAR);
 * </pre>
 * </p>
 *
 * @see MessagingClientProvider
 * @see ServiceLoader
 */
public final class ProviderRegistry {

    private static final Logger log = LoggerFactory.getLogger(ProviderRegistry.class);
    private static final ProviderRegistry INSTANCE = new ProviderRegistry();

    private final Map<MessagingProvider, MessagingClientProvider> providers;
    private final ReadWriteLock lock;
    private volatile boolean initialized;

    /**
     * Private constructor for singleton pattern.
     */
    private ProviderRegistry() {
        this.providers = new HashMap<>();
        this.lock = new ReentrantReadWriteLock();
        this.initialized = false;
    }

    /**
     * Returns the singleton instance of the provider registry.
     *
     * @return the provider registry instance
     */
    public static ProviderRegistry getInstance() {
        return INSTANCE;
    }

    /**
     * Gets the provider for the specified messaging provider type.
     * <p>
     * If providers have not been loaded yet, this method will trigger
     * discovery via ServiceLoader. The discovered providers are cached
     * for subsequent calls.
     * </p>
     *
     * @param provider the messaging provider type
     * @return the provider implementation
     * @throws IllegalArgumentException if provider is null
     * @throws IllegalStateException if no provider is found for the given type
     */
    public MessagingClientProvider getProvider(MessagingProvider provider) {
        if (provider == null) {
            throw new IllegalArgumentException("Provider cannot be null");
        }

        // Try read lock first (fast path for cached providers)
        lock.readLock().lock();
        try {
            if (initialized && providers.containsKey(provider)) {
                return providers.get(provider);
            }
        } finally {
            lock.readLock().unlock();
        }

        // Need to load providers (slow path)
        lock.writeLock().lock();
        try {
            // Double-check after acquiring write lock
            if (!initialized) {
                loadProviders();
                initialized = true;
            }

            MessagingClientProvider clientProvider = providers.get(provider);
            if (clientProvider == null) {
                throw new IllegalStateException(
                    "No provider implementation found for: " + provider +
                    ". Available providers: " + providers.keySet()
                );
            }
            return clientProvider;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Checks if a provider is registered for the given type.
     *
     * @param provider the messaging provider type
     * @return true if a provider is registered, false otherwise
     */
    public boolean hasProvider(MessagingProvider provider) {
        if (provider == null) {
            return false;
        }

        lock.readLock().lock();
        try {
            if (!initialized) {
                // Need to load providers first
                lock.readLock().unlock();
                lock.writeLock().lock();
                try {
                    if (!initialized) {
                        loadProviders();
                        initialized = true;
                    }
                    lock.readLock().lock();
                } finally {
                    lock.writeLock().unlock();
                }
            }
            return providers.containsKey(provider);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Registers a provider implementation.
     * <p>
     * This method is primarily for testing purposes. In production,
     * providers should be discovered via ServiceLoader.
     * </p>
     *
     * @param provider the provider implementation
     * @throws IllegalArgumentException if provider is null
     */
    public void registerProvider(MessagingClientProvider provider) {
        if (provider == null) {
            throw new IllegalArgumentException("Provider cannot be null");
        }

        lock.writeLock().lock();
        try {
            MessagingProvider type = provider.getProvider();
            if (providers.containsKey(type)) {
                log.warn("Overriding existing provider for type: {}", type);
            }
            providers.put(type, provider);
            log.info("Registered provider: {} for type: {}", 
                provider.getClass().getName(), type);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Clears all registered providers.
     * <p>
     * This method is primarily for testing purposes.
     * </p>
     */
    public void clear() {
        lock.writeLock().lock();
        try {
            providers.clear();
            initialized = false;
            log.debug("Cleared all registered providers");
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Returns the number of registered providers.
     *
     * @return the number of providers
     */
    public int getProviderCount() {
        lock.readLock().lock();
        try {
            if (!initialized) {
                lock.readLock().unlock();
                lock.writeLock().lock();
                try {
                    if (!initialized) {
                        loadProviders();
                        initialized = true;
                    }
                    lock.readLock().lock();
                } finally {
                    lock.writeLock().unlock();
                }
            }
            return providers.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Loads provider implementations using ServiceLoader.
     * <p>
     * This method must be called with write lock held.
     * </p>
     */
    private void loadProviders() {
        log.debug("Loading messaging client providers via ServiceLoader");
        
        ServiceLoader<MessagingClientProvider> loader = 
            ServiceLoader.load(MessagingClientProvider.class);
        
        int count = 0;
        for (MessagingClientProvider provider : loader) {
            try {
                MessagingProvider type = provider.getProvider();
                if (providers.containsKey(type)) {
                    log.warn("Duplicate provider found for type: {}. Using first discovered: {}", 
                        type, providers.get(type).getClass().getName());
                } else {
                    providers.put(type, provider);
                    log.info("Discovered provider: {} for type: {}", 
                        provider.getClass().getName(), type);
                    count++;
                }
            } catch (Exception e) {
                log.error("Failed to load provider: {}", 
                    provider.getClass().getName(), e);
            }
        }
        
        log.info("Loaded {} messaging client provider(s)", count);
        
        if (count == 0) {
            log.warn("No messaging client providers found. " +
                "Ensure provider implementations are on the classpath with proper " +
                "META-INF/services registration");
        }
    }
}

// Made with Bob
