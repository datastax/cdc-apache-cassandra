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

import com.datastax.oss.cdc.messaging.stats.ProducerStats;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Abstraction for message producer.
 * Publishes messages to topics with key-value pairs and properties.
 * 
 * <p>Producers are thread-safe and support both synchronous and asynchronous operations.
 * 
 * <p>Usage example:
 * <pre>{@code
 * MessageProducer<String, MyData> producer = client.createProducer(config);
 * try {
 *     Map<String, String> props = Map.of("token", "12345");
 *     MessageId id = producer.send("key1", data, props);
 *     System.out.println("Sent message: " + id);
 * } finally {
 *     producer.close();
 * }
 * }</pre>
 * 
 * @param <K> Key type
 * @param <V> Value type
 */
public interface MessageProducer<K, V> extends AutoCloseable {
    
    /**
     * Send a message asynchronously.
     * Non-blocking operation that returns immediately with a CompletableFuture.
     * 
     * @param key Message key for partitioning
     * @param value Message value (payload)
     * @param properties Message properties (metadata)
     * @return CompletableFuture that completes with MessageId when sent
     */
    CompletableFuture<MessageId> sendAsync(K key, V value, Map<String, String> properties);
    
    /**
     * Send a message synchronously.
     * Blocks until message is acknowledged by the broker.
     * 
     * @param key Message key for partitioning
     * @param value Message value (payload)
     * @param properties Message properties (metadata)
     * @return MessageId of sent message
     * @throws MessagingException if send fails
     */
    MessageId send(K key, V value, Map<String, String> properties) throws MessagingException;
    
    /**
     * Flush all pending messages.
     * Blocks until all buffered messages are sent.
     * 
     * @throws MessagingException if flush fails
     */
    void flush() throws MessagingException;
    
    /**
     * Get producer statistics.
     * 
     * @return ProducerStats instance with metrics
     */
    ProducerStats getStats();
    
    /**
     * Get the topic name this producer publishes to.
     * 
     * @return Topic name
     */
    String getTopic();
    
    /**
     * Check if producer is connected and ready.
     * 
     * @return true if ready to send messages
     */
    boolean isConnected();
    
    /**
     * Close the producer and release resources.
     * Flushes pending messages before closing.
     * 
     * @throws MessagingException if close fails
     */
    @Override
    void close() throws MessagingException;
}

// Made with Bob
