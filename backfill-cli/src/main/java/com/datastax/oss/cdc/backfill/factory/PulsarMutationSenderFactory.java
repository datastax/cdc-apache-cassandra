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
import org.apache.cassandra.schema.TableMetadata;

public class PulsarMutationSenderFactory {

    private final ImportSettings importSettings;

    public PulsarMutationSenderFactory(ImportSettings importSettings) {
        this.importSettings = importSettings;
    }

    /**
     * Creates a MutationSender via the messaging abstraction layer (Pulsar provider).
     * <p>
     * The version-specific {@code PulsarMutationSender} extends
     * {@code AbstractMessagingMutationSender}, which builds and owns its messaging client from an
     * {@link AgentConfig} (constructor {@code (AgentConfig, boolean)}). We therefore translate the
     * backfill {@link ImportSettings} into an {@link AgentConfig} rather than passing a pre-built
     * client. Murmur3 partitioning is disabled to use round-robin routing for backfill operations.
     */
    public MutationSender<TableMetadata> newPulsarMutationSender() {
        try {
            AgentConfig config = buildAgentConfig();

            // Use reflection to instantiate the appropriate PulsarMutationSender based on the
            // agent variant on the classpath (C3/C4/DSE4).
            String senderClassName = detectPulsarMutationSenderClass();

            Class<?> senderClass = Class.forName(senderClassName);
            java.lang.reflect.Constructor<?> constructor = senderClass.getConstructor(
                AgentConfig.class,
                boolean.class
            );

            @SuppressWarnings("unchecked")
            MutationSender<TableMetadata> sender = (MutationSender<TableMetadata>) constructor.newInstance(
                config,
                false  // Disable Murmur3 partitioner for round-robin routing
            );

            return sender;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create PulsarMutationSender via messaging abstraction", e);
        }
    }

    /**
     * Translate the backfill import settings into an {@link AgentConfig} targeting Pulsar.
     */
    private AgentConfig buildAgentConfig() {
        AgentConfig config = new AgentConfig();
        config.messagingProvider = "pulsar";
        config.topicPrefix = importSettings.topicPrefix;
        config.pulsarServiceUrl = importSettings.pulsarServiceUrl;
        config.pulsarAuthPluginClassName = importSettings.pulsarAuthPluginClassName;
        config.pulsarAuthParams = importSettings.pulsarAuthParams;

        // SSL / TLS
        config.sslProvider = importSettings.sslProvider;
        config.sslTruststorePath = importSettings.sslTruststorePath;
        config.sslTruststorePassword = importSettings.sslTruststorePassword;
        config.sslTruststoreType = importSettings.sslTruststoreType;
        config.sslKeystorePath = importSettings.sslKeystorePath;
        config.sslKeystorePassword = importSettings.sslKeystorePassword;
        config.sslCipherSuites = importSettings.sslCipherSuites;
        config.sslEnabledProtocols = importSettings.sslEnabledProtocols;
        config.sslAllowInsecureConnection = importSettings.sslAllowInsecureConnection;
        config.sslHostnameVerificationEnable = importSettings.sslHostnameVerificationEnable;
        config.tlsTrustCertsFilePath = importSettings.tlsTrustCertsFilePath;
        config.useKeyStoreTls = importSettings.useKeyStoreTls;
        return config;
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
