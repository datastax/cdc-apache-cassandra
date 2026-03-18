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

import com.datastax.oss.cdc.messaging.Message;
import com.datastax.oss.cdc.messaging.MessagingException;
import com.datastax.oss.cdc.messaging.config.ConsumerConfig;
import com.datastax.oss.cdc.messaging.impl.AbstractMessageConsumer;
import com.datastax.oss.cdc.messaging.stats.ConsumerStats;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

/**
 * Kafka implementation of MessageConsumer.
 * Stub implementation - will be completed in Day 3.
 * 
 * @param <K> Key type
 * @param <V> Value type
 */
public class KafkaMessageConsumer<K, V> extends AbstractMessageConsumer<K, V> {
    
    private final Properties kafkaProps;
    
    public KafkaMessageConsumer(Properties commonProps, ConsumerConfig<K, V> config) {
        super(config);
        this.kafkaProps = KafkaConfigMapper.buildConsumerProperties(commonProps, config);
        this.connected.set(true); // Mark as connected for stub
    }
    
    @Override
    protected Message<K, V> doReceive(Duration timeout) throws Exception {
        // TODO: Implement in Day 3
        throw new UnsupportedOperationException("Not yet implemented");
    }
    
    @Override
    protected CompletableFuture<Message<K, V>> doReceiveAsync() {
        // TODO: Implement in Day 3
        throw new UnsupportedOperationException("Not yet implemented");
    }
    
    @Override
    protected void doAcknowledge(Message<K, V> message) throws Exception {
        // TODO: Implement in Day 3
    }
    
    @Override
    protected CompletableFuture<Void> doAcknowledgeAsync(Message<K, V> message) {
        // TODO: Implement in Day 3
        throw new UnsupportedOperationException("Not yet implemented");
    }
    
    @Override
    protected void doNegativeAcknowledge(Message<K, V> message) throws Exception {
        // TODO: Implement in Day 3
    }
    
    @Override
    protected void doClose() throws Exception {
        // TODO: Implement in Day 3
    }
    
    @Override
    public ConsumerStats getStats() {
        // TODO: Implement in Day 3
        return null;
    }
}

// Made with Bob