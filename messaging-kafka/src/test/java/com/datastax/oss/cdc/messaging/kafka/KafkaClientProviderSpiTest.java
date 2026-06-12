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

import com.datastax.oss.cdc.messaging.config.MessagingProvider;
import com.datastax.oss.cdc.messaging.factory.ProviderRegistry;
import com.datastax.oss.cdc.messaging.spi.MessagingClientProvider;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the Kafka provider is discoverable through the Java ServiceLoader SPI.
 */
public class KafkaClientProviderSpiTest {

    @Test
    public void kafkaProviderShouldBeDiscoverableViaSpi() {
        ProviderRegistry registry = ProviderRegistry.getInstance();
        assertTrue(registry.hasProvider(MessagingProvider.KAFKA),
                "KafkaClientProvider should be registered via META-INF/services");

        MessagingClientProvider provider = registry.getProvider(MessagingProvider.KAFKA);
        assertEquals(MessagingProvider.KAFKA, provider.getProvider());
        assertTrue(provider instanceof KafkaClientProvider);
        assertTrue(provider.supports(MessagingProvider.KAFKA));
    }
}
