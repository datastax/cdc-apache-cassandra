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
import com.datastax.oss.cdc.agent.KafkaMutationSender;
import com.datastax.oss.cdc.backfill.importer.ImportSettings;

import java.util.HashMap;
import java.util.Map;

public class KafkaMutationSenderFactory {

    private final ImportSettings importSettings;

    public KafkaMutationSenderFactory(ImportSettings importSettings) {
        this.importSettings = importSettings;
    }

    // Disable Murmur3 partitioner — backfill uses round-robin, same as PulsarMutationSenderFactory.
    public KafkaMutationSender newKafkaMutationSender() {
        return new KafkaMutationSender(createAgentConfig(), false);
    }

    private AgentConfig createAgentConfig() {
        Map<String, Object> params = new HashMap<>();
        params.put(AgentConfig.KAFKA_CONFIG_FILE, importSettings.kafkaConfigFile);
        params.put(AgentConfig.TOPIC_PREFIX, importSettings.topicPrefix);
        return AgentConfig.create(AgentConfig.Platform.KAFKA, params);
    }
}
