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
import com.datastax.oss.cdc.messaging.kafka.serde.KafkaSerde;
import com.datastax.oss.cdc.messaging.kafka.serde.RawAvroSerde;
import com.datastax.oss.cdc.messaging.kafka.serde.RegistryAvroSerde;
import com.datastax.oss.cdc.messaging.stats.ClientStats;
import com.datastax.oss.cdc.messaging.stats.impl.BaseClientStats;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Kafka-specific implementation of MessagingClient.
 * Manages Kafka producer and consumer lifecycle.
 * 
 * <p>Unlike Pulsar, Kafka doesn't have a central client object.
 * This class manages common configuration and creates individual producers/consumers.
 * 
 * <p>Thread-safe.
 */
public class KafkaMessagingClient extends AbstractMessagingClient {
    
    private static final Logger log = LoggerFactory.getLogger(KafkaMessagingClient.class);
    
    private Properties commonProperties;
    private KafkaSerde serde;
    private final BaseClientStats stats;
    
    /**
     * Create KafkaMessagingClient.
     * Call {@link #initialize(ClientConfig)} before use.
     */
    public KafkaMessagingClient() {
        this.stats = new BaseClientStats();
    }
    
    @Override
    protected void doInitialize(ClientConfig config) throws Exception {
        log.info("Initializing Kafka client with bootstrap servers: {}", config.getServiceUrl());
        
        try {
            // Map configuration to Kafka common properties
            this.commonProperties = KafkaConfigMapper.mapClientConfig(config);
            
            // Select the serde strategy: Confluent Schema Registry when a registry URL is
            // configured, otherwise registry-less raw AVRO (works with plain Apache Kafka).
            Object schemaRegistryUrlObj = config.getProviderProperties() != null ?
                config.getProviderProperties().get("schema.registry.url") : null;
            String schemaRegistryUrl = schemaRegistryUrlObj != null ?
                schemaRegistryUrlObj.toString().trim() : null;

            if (schemaRegistryUrl != null && !schemaRegistryUrl.isEmpty()) {
                this.serde = new RegistryAvroSerde(schemaRegistryUrl, config.getProviderProperties());
                log.info("Using Confluent Schema Registry serde: {}", schemaRegistryUrl);
            } else {
                this.serde = new RawAvroSerde();
                log.info("No schema registry URL configured; using registry-less raw AVRO serde");
            }

            log.info("Kafka client initialized successfully");
        } catch (Exception e) {
            log.error("Failed to initialize Kafka client", e);
            throw e;
        }
    }
    
    @Override
    protected <K, V> MessageProducer<K, V> doCreateProducer(ProducerConfig<K, V> config) 
            throws Exception {
        log.debug("Creating Kafka producer for topic: {}", config.getTopic());
        
        try {
            // Map configuration to Kafka producer properties
            Properties producerProps = KafkaConfigMapper.mapProducerConfig(
                this.config, config);
            
            // Create Kafka producer
            KafkaProducer<byte[], byte[]> kafkaProducer = 
                new KafkaProducer<>(producerProps);
            
            // Create wrapper
            KafkaMessageProducer<K, V> producer = new KafkaMessageProducer<>(
                kafkaProducer, config, serde);
            
            stats.incrementProducerCount();
            
            log.info("Created Kafka producer for topic: {}", config.getTopic());
            return producer;
            
        } catch (Exception e) {
            log.error("Failed to create Kafka producer for topic: {}", config.getTopic(), e);
            throw e;
        }
    }
    
    @Override
    protected <K, V> MessageConsumer<K, V> doCreateConsumer(ConsumerConfig<K, V> config) 
            throws Exception {
        log.debug("Creating Kafka consumer for topic: {}", config.getTopic());
        
        try {
            // Map configuration to Kafka consumer properties
            Properties consumerProps = KafkaConfigMapper.mapConsumerConfig(
                this.config, config);
            
            // Create Kafka consumer
            KafkaConsumer<byte[], byte[]> kafkaConsumer =
                new KafkaConsumer<>(consumerProps);
            
            // Create wrapper
            KafkaMessageConsumer<K, V> consumer = new KafkaMessageConsumer<>(
                kafkaConsumer, config, serde);
            
            stats.incrementConsumerCount();
            
            log.info("Created Kafka consumer for topic: {}", config.getTopic());
            return consumer;
            
        } catch (Exception e) {
            log.error("Failed to create Kafka consumer for topic: {}", config.getTopic(), e);
            throw e;
        }
    }
    
    @Override
    protected void doClose() throws Exception {
        try {
            if (serde != null) {
                serde.close();
            }
            log.info("Closed Kafka client");
        } catch (Exception e) {
            log.error("Error closing Kafka client", e);
            throw e;
        }
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
     * Get the common Kafka properties.
     */
    public Properties getCommonProperties() {
        return commonProperties;
    }
    
    /**
     * Get the serde strategy in use (registry-less or Confluent Schema Registry).
     */
    public KafkaSerde getSerde() {
        return serde;
    }
}

