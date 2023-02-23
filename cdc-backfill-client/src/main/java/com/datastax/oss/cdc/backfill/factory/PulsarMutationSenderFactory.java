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
import com.datastax.oss.cdc.agent.PulsarMutationSender;
import com.datastax.oss.cdc.backfill.ImportSettings;

public class PulsarMutationSenderFactory {

    private final ImportSettings importSettings;

    public PulsarMutationSenderFactory(ImportSettings importSettings) {

        this.importSettings = importSettings;
    }

    // 1. Disable Murmur3 partitioner usages. This will default to round-robin in pulsar producer.
    // 2. A git diff between C3/C4/DSE4 on PulsarMutationSender shows no difference. Here we use the dse one.
    // TODO: Add e2e tests to verify compatibility with C3/C4/DSE4.
    public PulsarMutationSender newPulsarMutationSender() {
        return new PulsarMutationSender(createAgentConfigs(), false);
    }

    private AgentConfig createAgentConfigs() {
        AgentConfig configs = new AgentConfig();
        configs.pulsarServiceUrl = importSettings.pulsarServiceUrl;

        configs.sslTruststorePath = importSettings.sslTruststorePath;
        configs.sslTruststorePassword = importSettings.sslTruststorePassword;

        configs.sslKeystorePath = importSettings.sslKeystorePath;
        configs.sslKeystorePassword = importSettings.sslKeystorePassword;
        configs.sslTruststoreType = importSettings.sslTruststoreType;
        configs.tlsTrustCertsFilePath = importSettings.tlsTrustCertsFilePath;
        configs.useKeyStoreTls = importSettings.useKeyStoreTls;
        configs.sslAllowInsecureConnection = importSettings.sslAllowInsecureConnection;
        configs.sslHostnameVerificationEnable = importSettings.sslHostnameVerificationEnable;

        configs.sslProvider = importSettings.sslProvider;
        configs.sslCipherSuites = importSettings.sslCipherSuites;
        configs.sslEnabledProtocols = importSettings.sslEnabledProtocols;

        configs.pulsarAuthPluginClassName = importSettings.pulsarAuthPluginClassName;
        configs.pulsarAuthParams = importSettings.pulsarAuthParams;

        configs.topicPrefix = importSettings.topicPrefix;
        return configs;
    }
}
