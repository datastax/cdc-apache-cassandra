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

import com.datastax.oss.cdc.messaging.MessagingClient;
import com.datastax.oss.cdc.messaging.MessagingException;
import com.datastax.oss.cdc.messaging.config.ClientConfig;
import com.datastax.oss.cdc.messaging.config.MessagingProvider;
import com.datastax.oss.cdc.messaging.spi.MessagingClientProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SPI implementation for Kafka messaging provider.
 * Discovered via Java ServiceLoader mechanism.
 *
 * <p>Thread-safe.
 */
public class KafkaClientProvider implements MessagingClientProvider {
    
    private static final Logger log = LoggerFactory.getLogger(KafkaClientProvider.class);
    
    @Override
    public MessagingProvider getProvider() {
        return MessagingProvider.KAFKA;
    }
    
    @Override
    public MessagingClient createClient(ClientConfig config) throws MessagingException {
        log.debug("Creating Kafka messaging client");
        KafkaMessagingClient client = new KafkaMessagingClient();
        client.initialize(config);
        return client;
    }
}

// Made with Bob
