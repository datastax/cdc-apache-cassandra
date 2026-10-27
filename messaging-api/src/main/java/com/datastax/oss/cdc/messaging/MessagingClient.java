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
package com.datastax.oss.cdc.messaging;

import com.datastax.oss.cdc.messaging.config.ClientConfig;
import com.datastax.oss.cdc.messaging.config.ConsumerConfig;
import com.datastax.oss.cdc.messaging.config.ProducerConfig;
import com.datastax.oss.cdc.messaging.stats.ClientStats;

/**
 * Abstraction for messaging platform client.
 * Manages connection lifecycle and creates producers/consumers.
 * 
 * <p>The client is the entry point for all messaging operations.
 * It manages the connection to the messaging platform and provides
 * factory methods for creating producers and consumers.
 * 
 * <p>Implementations are thread-safe and can be shared across threads.
 * 
 * <p>Usage example:
 * <pre>{@code
 * ClientConfig config = ClientConfig.builder()
 *     .serviceUrl("pulsar://localhost:6650")
 *     .build();
 * 
 * MessagingClient client = MessagingClientFactory.create(config);
 * try {
 *     client.initialize(config);
 *     
 *     MessageProducer<String, MyData> producer = 
 *         client.createProducer(producerConfig);
 *     
 *     MessageConsumer<String, MyData> consumer = 
 *         client.createConsumer(consumerConfig);
 *     
 *     // Use producer and consumer...
 * } finally {
 *     client.close();
 * }
 * }</pre>
 */
public interface MessagingClient extends AutoCloseable {
    
    /**
     * Initialize the client with configuration.
     * Must be called before creating producers or consumers.
     * 
     * @param config Client configuration
     * @throws MessagingException if initialization fails
     */
    void initialize(ClientConfig config) throws MessagingException;
    
    /**
     * Create a message producer.
     * Producer is ready to send messages immediately.
     * 
     * @param <K> Key type
     * @param <V> Value type
     * @param config Producer configuration
     * @return MessageProducer instance
     * @throws MessagingException if creation fails
     */
    <K, V> MessageProducer<K, V> createProducer(ProducerConfig<K, V> config) 
        throws MessagingException;
    
    /**
     * Create a message consumer.
     * Consumer is ready to receive messages immediately.
     * 
     * @param <K> Key type
     * @param <V> Value type
     * @param config Consumer configuration
     * @return MessageConsumer instance
     * @throws MessagingException if creation fails
     */
    <K, V> MessageConsumer<K, V> createConsumer(ConsumerConfig<K, V> config) 
        throws MessagingException;
    
    /**
     * Get client statistics.
     * 
     * @return ClientStats instance with metrics
     */
    ClientStats getStats();
    
    /**
     * Check if client is connected to the messaging platform.
     * 
     * @return true if connected and operational
     */
    boolean isConnected();
    
    /**
     * Get the messaging provider type.
     * 
     * @return Provider type (e.g., "pulsar", "kafka")
     */
    String getProviderType();
    
    /**
     * Close the client and release all resources.
     * Closes all producers and consumers created by this client.
     * 
     * @throws MessagingException if close fails
     */
    @Override
    void close() throws MessagingException;
}

