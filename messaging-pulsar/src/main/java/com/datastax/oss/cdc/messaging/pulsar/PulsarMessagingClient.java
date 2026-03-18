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
import org.apache.pulsar.client.api.PulsarClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pulsar implementation of MessagingClient.
 * Wraps Apache Pulsar client and provides messaging abstraction.
 * 
 * <p>Thread-safe. Manages Pulsar client lifecycle and creates producers/consumers.
 */
public class PulsarMessagingClient extends AbstractMessagingClient {
    
    private static final Logger log = LoggerFactory.getLogger(PulsarMessagingClient.class);
    
    private PulsarClient pulsarClient;
    private final BaseClientStats stats = new BaseClientStats();
    
    @Override
    protected void doInitialize(ClientConfig config) throws Exception {
        log.info("Initializing Pulsar client with service URL: {}", config.getServiceUrl());
        
        // Use PulsarConfigMapper to build Pulsar client
        this.pulsarClient = PulsarConfigMapper.mapClientConfig(config).build();
        
        log.info("Pulsar client initialized successfully");
    }
    
    @Override
    protected <K, V> MessageProducer<K, V> doCreateProducer(ProducerConfig<K, V> config)
            throws Exception {
        log.debug("Creating Pulsar producer for topic: {}", config.getTopic());
        
        PulsarMessageProducer<K, V> producer = new PulsarMessageProducer<>(pulsarClient, config);
        stats.incrementProducerCount();
        
        return producer;
    }
    
    @Override
    protected <K, V> MessageConsumer<K, V> doCreateConsumer(ConsumerConfig<K, V> config)
            throws Exception {
        log.debug("Creating Pulsar consumer for topic: {}, subscription: {}",
            config.getTopic(), config.getSubscriptionName());
        
        PulsarMessageConsumer<K, V> consumer = new PulsarMessageConsumer<>(pulsarClient, config);
        stats.incrementConsumerCount();
        
        return consumer;
    }
    
    @Override
    protected void doClose() throws Exception {
        if (pulsarClient != null) {
            log.info("Closing Pulsar client");
            pulsarClient.close();
            pulsarClient = null;
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
     * Get underlying Pulsar client.
     * For internal use only.
     * 
     * @return PulsarClient instance
     */
    PulsarClient getPulsarClient() {
        return pulsarClient;
    }
}

// Made with Bob
