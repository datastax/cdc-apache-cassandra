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

import com.datastax.oss.cdc.messaging.stats.ConsumerStats;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Abstraction for message consumer.
 * Consumes messages from topics with acknowledgment support.
 * 
 * <p>Consumers support:
 * <ul>
 *   <li>Synchronous and asynchronous message reception</li>
 *   <li>Individual message acknowledgment</li>
 *   <li>Negative acknowledgment for retry</li>
 *   <li>Subscription-based consumption</li>
 * </ul>
 * 
 * <p>Usage example:
 * <pre>{@code
 * MessageConsumer<String, MyData> consumer = client.createConsumer(config);
 * try {
 *     while (running) {
 *         Message<String, MyData> msg = consumer.receive(Duration.ofSeconds(1));
 *         if (msg != null) {
 *             process(msg);
 *             consumer.acknowledge(msg);
 *         }
 *     }
 * } finally {
 *     consumer.close();
 * }
 * }</pre>
 * 
 * @param <K> Key type
 * @param <V> Value type
 */
public interface MessageConsumer<K, V> extends AutoCloseable {
    
    /**
     * Receive a message with timeout.
     * Blocks until a message is available or timeout expires.
     * 
     * @param timeout Maximum wait time
     * @return Message or null if timeout
     * @throws MessagingException if receive fails
     */
    Message<K, V> receive(Duration timeout) throws MessagingException;
    
    /**
     * Receive a message asynchronously.
     * Returns immediately with a CompletableFuture.
     * 
     * @return CompletableFuture that completes with Message
     */
    CompletableFuture<Message<K, V>> receiveAsync();
    
    /**
     * Acknowledge successful message processing.
     * Tells the broker that the message was processed successfully.
     * 
     * @param message Message to acknowledge
     * @throws MessagingException if acknowledgment fails
     */
    void acknowledge(Message<K, V> message) throws MessagingException;
    
    /**
     * Acknowledge message asynchronously.
     * 
     * @param message Message to acknowledge
     * @return CompletableFuture for acknowledgment completion
     */
    CompletableFuture<Void> acknowledgeAsync(Message<K, V> message);
    
    /**
     * Negative acknowledge (requeue for retry).
     * Tells the broker that message processing failed and should be retried.
     * 
     * @param message Message to negative acknowledge
     * @throws MessagingException if negative acknowledgment fails
     */
    void negativeAcknowledge(Message<K, V> message) throws MessagingException;
    
    /**
     * Get consumer statistics.
     * 
     * @return ConsumerStats instance with metrics
     */
    ConsumerStats getStats();
    
    /**
     * Get the subscription name.
     * 
     * @return Subscription name
     */
    String getSubscription();
    
    /**
     * Get the topic(s) this consumer subscribes to.
     * 
     * @return Topic name or pattern
     */
    String getTopic();
    
    /**
     * Check if consumer is connected and ready.
     * 
     * @return true if ready to receive messages
     */
    boolean isConnected();
    
    /**
     * Close the consumer and release resources.
     * 
     * @throws MessagingException if close fails
     */
    @Override
    void close() throws MessagingException;
}

