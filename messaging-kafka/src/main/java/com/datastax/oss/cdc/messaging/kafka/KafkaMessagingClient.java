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
package com.datastax.oss.cdc.messaging.kafka;

import com.datastax.oss.cdc.messaging.MessageConsumer;
import com.datastax.oss.cdc.messaging.MessageProducer;
import com.datastax.oss.cdc.messaging.config.ClientConfig;
import com.datastax.oss.cdc.messaging.config.ConsumerConfig;
import com.datastax.oss.cdc.messaging.config.ProducerConfig;
import com.datastax.oss.cdc.messaging.impl.AbstractMessagingClient;
import com.datastax.oss.cdc.messaging.stats.ClientStats;
import com.datastax.oss.cdc.messaging.stats.impl.BaseClientStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Kafka implementation of MessagingClient.
 * Manages Kafka producers and consumers through the messaging abstraction.
 * 
 * <p>Thread-safe. Manages Kafka client lifecycle and creates producers/consumers.
 * 
 * <p>Unlike Pulsar which has a single client instance, Kafka uses separate
 * producer and consumer instances. This class manages the common configuration
 * and creates instances as needed.
 */
public class KafkaMessagingClient extends AbstractMessagingClient {
    
    private static final Logger log = LoggerFactory.getLogger(KafkaMessagingClient.class);
    
    private Properties commonProperties;
    private final BaseClientStats stats = new BaseClientStats();
    
    @Override
    protected void doInitialize(ClientConfig config) throws Exception {
        log.info("Initializing Kafka client with bootstrap servers: {}", config.getServiceUrl());
        
        // Build common Kafka properties from client config
        this.commonProperties = KafkaConfigMapper.buildCommonProperties(config);
        
        log.info("Kafka client initialized successfully");
    }
    
    @Override
    protected <K, V> MessageProducer<K, V> doCreateProducer(ProducerConfig<K, V> config)
            throws Exception {
        log.debug("Creating Kafka producer for topic: {}", config.getTopic());
        
        KafkaMessageProducer<K, V> producer = new KafkaMessageProducer<>(commonProperties, config);
        stats.incrementProducerCount();
        
        return producer;
    }
    
    @Override
    protected <K, V> MessageConsumer<K, V> doCreateConsumer(ConsumerConfig<K, V> config)
            throws Exception {
        log.debug("Creating Kafka consumer for topic: {}, subscription: {}",
            config.getTopic(), config.getSubscriptionName());
        
        KafkaMessageConsumer<K, V> consumer = new KafkaMessageConsumer<>(commonProperties, config);
        stats.incrementConsumerCount();
        
        return consumer;
    }
    
    @Override
    protected void doClose() throws Exception {
        log.info("Closing Kafka client");
        // Kafka doesn't have a central client to close
        // Individual producers and consumers are closed by AbstractMessagingClient
        commonProperties = null;
    }
    
    @Override
    public ClientStats getStats() {
        return stats;
    }
    
    @Override
    public String getProviderType() {
        return "kafka";
    }
    
    /**
     * Get common Kafka properties.
     * For internal use only.
     * 
     * @return Common Kafka properties
     */
    Properties getCommonProperties() {
        return commonProperties;
    }
}

// Made with Bob