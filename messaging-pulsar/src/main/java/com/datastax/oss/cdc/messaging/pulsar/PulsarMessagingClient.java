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

import com.datastax.oss.cdc.messaging.MessageConsumer;
import com.datastax.oss.cdc.messaging.MessageProducer;
import com.datastax.oss.cdc.messaging.config.ClientConfig;
import com.datastax.oss.cdc.messaging.config.ConsumerConfig;
import com.datastax.oss.cdc.messaging.config.ProducerConfig;
import com.datastax.oss.cdc.messaging.impl.AbstractMessagingClient;
import com.datastax.oss.cdc.messaging.stats.ClientStats;
import com.datastax.oss.cdc.messaging.stats.impl.BaseClientStats;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.common.schema.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pulsar-specific implementation of MessagingClient.
 * Manages Pulsar client lifecycle and creates Pulsar producers/consumers.
 * 
 * <p>Thread-safe.
 */
public class PulsarMessagingClient extends AbstractMessagingClient {
    
    private static final Logger log = LoggerFactory.getLogger(PulsarMessagingClient.class);
    
    private PulsarClient pulsarClient;
    private final BaseClientStats stats;
    
    /**
     * Create PulsarMessagingClient.
     * Call {@link #initialize(ClientConfig)} before use.
     */
    public PulsarMessagingClient() {
        this.stats = new BaseClientStats();
    }
    
    @Override
    protected void doInitialize(ClientConfig config) throws Exception {
        log.info("Initializing Pulsar client with service URL: {}", config.getServiceUrl());
        
        try {
            // Map configuration to Pulsar ClientBuilder
            ClientBuilder clientBuilder = PulsarConfigMapper.mapClientConfig(config);
            
            // Create Pulsar client
            this.pulsarClient = clientBuilder.build();
            
            log.info("Pulsar client initialized successfully");
        } catch (PulsarClientException e) {
            log.error("Failed to initialize Pulsar client", e);
            throw e;
        }
    }
    
    @Override
    protected <K, V> MessageProducer<K, V> doCreateProducer(ProducerConfig<K, V> config) throws Exception {
        log.debug("Creating Pulsar producer for topic: {}", config.getTopic());
        
        try {
            // Map configuration to Pulsar ProducerBuilder
            ProducerBuilder<KeyValue<K, V>> producerBuilder = 
                    PulsarConfigMapper.mapProducerConfig(pulsarClient, config);
            
            // Create Pulsar producer
            Producer<KeyValue<K, V>> pulsarProducer = producerBuilder.create();
            
            // Wrap in our abstraction
            PulsarMessageProducer<K, V> producer = new PulsarMessageProducer<>(config, pulsarProducer);
            
            stats.incrementProducerCount();
            log.debug("Pulsar producer created for topic: {}", config.getTopic());
            
            return producer;
        } catch (PulsarClientException e) {
            log.error("Failed to create Pulsar producer for topic: {}", config.getTopic(), e);
            throw e;
        }
    }
    
    @Override
    protected <K, V> MessageConsumer<K, V> doCreateConsumer(ConsumerConfig<K, V> config) throws Exception {
        log.debug("Creating Pulsar consumer for topic: {}, subscription: {}", 
                config.getTopic(), config.getSubscriptionName());
        
        try {
            // Map configuration to Pulsar ConsumerBuilder
            ConsumerBuilder<KeyValue<K, V>> consumerBuilder = 
                    PulsarConfigMapper.mapConsumerConfig(pulsarClient, config);
            
            // Create Pulsar consumer
            Consumer<KeyValue<K, V>> pulsarConsumer = consumerBuilder.subscribe();
            
            // Wrap in our abstraction
            PulsarMessageConsumer<K, V> consumer = new PulsarMessageConsumer<>(config, pulsarConsumer);
            
            stats.incrementConsumerCount();
            log.debug("Pulsar consumer created for subscription: {}", config.getSubscriptionName());
            
            return consumer;
        } catch (PulsarClientException e) {
            log.error("Failed to create Pulsar consumer for subscription: {}", 
                    config.getSubscriptionName(), e);
            throw e;
        }
    }
    
    @Override
    protected void doClose() throws Exception {
        if (pulsarClient != null) {
            try {
                pulsarClient.close();
                log.info("Pulsar client closed successfully");
            } catch (PulsarClientException e) {
                log.error("Error closing Pulsar client", e);
                throw e;
            }
        }
    }
    
    @Override
    public ClientStats getStats() {
        return stats;
    }
    
    @Override
    public String getProviderType() {
        return "pulsar";
    }
    
    /**
     * Get the underlying Pulsar client.
     * Used for Pulsar-specific operations.
     * 
     * @return PulsarClient instance or null if not initialized
     */
    public PulsarClient getPulsarClient() {
        return pulsarClient;
    }
    
    @Override
    public boolean isConnected() {
        return super.isConnected() && pulsarClient != null;
    }
}

