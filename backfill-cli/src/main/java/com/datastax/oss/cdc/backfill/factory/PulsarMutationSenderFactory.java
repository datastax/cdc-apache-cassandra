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

package com.datastax.oss.cdc.backfill.factory;

import com.datastax.oss.cdc.agent.AgentConfig;
import com.datastax.oss.cdc.agent.MutationSender;
import com.datastax.oss.cdc.backfill.importer.ImportSettings;
import com.datastax.oss.cdc.messaging.MessagingClient;
import com.datastax.oss.cdc.messaging.config.ClientConfig;
import com.datastax.oss.cdc.messaging.config.MessagingProvider;
import com.datastax.oss.cdc.messaging.config.impl.ClientConfigBuilder;
import com.datastax.oss.cdc.messaging.factory.MessagingClientFactory;
import org.apache.cassandra.schema.TableMetadata;

public class PulsarMutationSenderFactory {

    private final ImportSettings importSettings;

    public PulsarMutationSenderFactory(ImportSettings importSettings) {
        this.importSettings = importSettings;
    }

    /**
     * Creates a MutationSender using the messaging abstraction layer.
     * Disables Murmur3 partitioner to use round-robin routing for backfill operations.
     */
    public MutationSender<TableMetadata> newPulsarMutationSender() {
        try {
            // Create messaging client configuration
            ClientConfig clientConfig = ClientConfigBuilder.builder()
                .provider(MessagingProvider.PULSAR)
                .serviceUrl(importSettings.pulsarServiceUrl)
                .build();
            
            // Create messaging client using the factory
            MessagingClient messagingClient = MessagingClientFactory.create(clientConfig);
            
            // Use reflection to instantiate the appropriate PulsarMutationSender based on available classes
            // This maintains compatibility with C3/C4/DSE4 variants
            String senderClassName = detectPulsarMutationSenderClass();
            
            Class<?> senderClass = Class.forName(senderClassName);
            java.lang.reflect.Constructor<?> constructor = senderClass.getConstructor(
                MessagingClient.class,
                boolean.class
            );
            
            @SuppressWarnings("unchecked")
            MutationSender<TableMetadata> sender = (MutationSender<TableMetadata>) constructor.newInstance(
                messagingClient,
                false  // Disable Murmur3 partitioner for round-robin routing
            );
            
            return sender;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create PulsarMutationSender via messaging abstraction", e);
        }
    }

    /**
     * Detects which PulsarMutationSender class is available on the classpath.
     * Tries C4 first, then C3, then DSE4.
     */
    private String detectPulsarMutationSenderClass() {
        String[] candidates = {
            "com.datastax.oss.cdc.agent.PulsarMutationSender",  // C4 (default)
            "org.apache.cassandra.db.commitlog.PulsarMutationSender"  // Fallback
        };
        
        for (String className : candidates) {
            try {
                Class.forName(className);
                return className;
            } catch (ClassNotFoundException e) {
                // Try next candidate
            }
        }
        
        // Default to C4 if none found (will fail later with clear error)
        return "com.datastax.oss.cdc.agent.PulsarMutationSender";
    }
}
