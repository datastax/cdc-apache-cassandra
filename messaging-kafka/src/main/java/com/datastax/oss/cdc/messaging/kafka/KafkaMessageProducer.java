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

import com.datastax.oss.cdc.messaging.MessageId;
import com.datastax.oss.cdc.messaging.MessagingException;
import com.datastax.oss.cdc.messaging.config.ProducerConfig;
import com.datastax.oss.cdc.messaging.impl.AbstractMessageProducer;
import com.datastax.oss.cdc.messaging.stats.ProducerStats;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

/**
 * Kafka implementation of MessageProducer.
 * Stub implementation - will be completed in Day 2.
 * 
 * @param <K> Key type
 * @param <V> Value type
 */
public class KafkaMessageProducer<K, V> extends AbstractMessageProducer<K, V> {
    
    private final Properties kafkaProps;
    
    public KafkaMessageProducer(Properties commonProps, ProducerConfig<K, V> config) {
        super(config);
        this.kafkaProps = KafkaConfigMapper.buildProducerProperties(commonProps, config);
        this.connected.set(true); // Mark as connected for stub
    }
    
    @Override
    protected CompletableFuture<MessageId> doSendAsync(K key, V value, Map<String, String> properties) {
        // TODO: Implement in Day 2
        throw new UnsupportedOperationException("Not yet implemented");
    }
    
    @Override
    protected void doFlush() throws Exception {
        // TODO: Implement in Day 2
    }
    
    @Override
    protected void doClose() throws Exception {
        // TODO: Implement in Day 2
    }
    
    @Override
    public ProducerStats getStats() {
        // TODO: Implement in Day 2
        return null;
    }
}

// Made with Bob