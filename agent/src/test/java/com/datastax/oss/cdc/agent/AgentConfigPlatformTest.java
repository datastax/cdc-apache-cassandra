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
package com.datastax.oss.cdc.agent;

import org.junit.jupiter.api.Test;

import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers platform selection and the parameter validation in {@link AgentConfig#configure}.
 */
class AgentConfigPlatformTest {

    @Test
    void extractsPlatformFromAgentArgs() {
        assertEquals(AgentConfig.Platform.PULSAR, AgentConfig.extractPlatform(null));
        assertEquals(AgentConfig.Platform.PULSAR, AgentConfig.extractPlatform("topicPrefix=events-"));
        assertEquals(AgentConfig.Platform.KAFKA, AgentConfig.extractPlatform("topicPrefix=events-, platform=kafka"));
        assertEquals(AgentConfig.Platform.PULSAR, AgentConfig.extractPlatform("platform=Pulsar"));
    }

    @Test
    void ignoresNullParameterValues() {
        Map<String, Object> params = new HashMap<>();
        params.put(AgentConfig.TOPIC_PREFIX, null);

        AgentConfig config = AgentConfig.create(AgentConfig.Platform.PULSAR, params);

        assertEquals("events-", config.topicPrefix);
    }

    @Test
    void rejectsNonStringParameter() {
        Map<String, Object> params = new HashMap<>();
        params.put(AgentConfig.TOPIC_PREFIX, 42);

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> AgentConfig.create(AgentConfig.Platform.PULSAR, params));
        assertTrue(e.getMessage().contains("only String values are allowed"));
    }

    @Test
    void rejectsParameterOfTheOtherPlatform() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> AgentConfig.create(AgentConfig.Platform.PULSAR, AgentConfig.KAFKA_MUTATION_SEND_RATE_LIMIT + "=5"));
        assertTrue(e.getMessage().contains("PULSAR"));
    }

    @Test
    void rejectsUnknownParameter() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> AgentConfig.create(AgentConfig.Platform.KAFKA, "noSuchSetting=1"));
        assertTrue(e.getMessage().contains("noSuchSetting"));
    }

    @Test
    void failsClearlyWhenConfigFileIsMissing() {
        UncheckedIOException e = assertThrows(UncheckedIOException.class,
                () -> AgentConfig.create(AgentConfig.Platform.KAFKA, "kafkaConfigFile=/does/not/exist.properties"));
        assertTrue(e.getMessage().contains("/does/not/exist.properties"));
    }
}
