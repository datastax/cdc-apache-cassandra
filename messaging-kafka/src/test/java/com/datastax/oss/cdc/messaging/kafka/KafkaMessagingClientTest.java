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

import com.datastax.oss.cdc.messaging.config.ClientConfig;
import com.datastax.oss.cdc.messaging.config.MessagingProvider;
import com.datastax.oss.cdc.messaging.config.impl.ClientConfigBuilder;
import com.datastax.oss.cdc.messaging.kafka.serde.RawAvroSerde;
import com.datastax.oss.cdc.messaging.kafka.serde.RegistryAvroSerde;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the Kafka client selects the right serde based on schema-registry configuration.
 */
public class KafkaMessagingClientTest {

    private ClientConfig kafkaConfig(java.util.Map<String, Object> providerProps) {
        return ClientConfigBuilder.builder()
                .provider(MessagingProvider.KAFKA)
                .serviceUrl("localhost:9092")
                .providerProperties(providerProps)
                .build();
    }

    @Test
    public void shouldUseRawAvroSerdeWhenNoRegistryConfigured() throws Exception {
        KafkaMessagingClient client = new KafkaMessagingClient();
        client.initialize(kafkaConfig(Collections.emptyMap()));
        assertTrue(client.getSerde() instanceof RawAvroSerde,
                "Expected registry-less RawAvroSerde when no schema.registry.url is set");
        assertEquals("kafka", client.getProviderType());
    }

    @Test
    public void shouldUseRegistrySerdeWhenRegistryConfigured() throws Exception {
        KafkaMessagingClient client = new KafkaMessagingClient();
        client.initialize(kafkaConfig(Collections.singletonMap(
                "schema.registry.url", "http://localhost:8081")));
        assertTrue(client.getSerde() instanceof RegistryAvroSerde,
                "Expected Confluent RegistryAvroSerde when schema.registry.url is set");
    }

    @Test
    public void blankRegistryUrlShouldFallBackToRawSerde() throws Exception {
        KafkaMessagingClient client = new KafkaMessagingClient();
        client.initialize(kafkaConfig(Collections.singletonMap("schema.registry.url", "   ")));
        assertTrue(client.getSerde() instanceof RawAvroSerde);
    }
}
