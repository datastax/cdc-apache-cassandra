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
import com.datastax.oss.cdc.messaging.MessagingClient;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Verifies the contract {@link PulsarMutationSenderFactory} relies on: the agent's
 * {@code PulsarMutationSender} (which extends {@code AbstractMessagingMutationSender} and owns its
 * messaging client) exposes an {@code (AgentConfig, boolean)} constructor — NOT the
 * {@code (MessagingClient, boolean)} constructor the factory previously (incorrectly) reflected on.
 */
public class PulsarMutationSenderFactoryTest {

    private static final String SENDER_CLASS = "com.datastax.oss.cdc.agent.PulsarMutationSender";

    @Test
    public void senderExposesAgentConfigConstructor() throws Exception {
        Class<?> senderClass = Class.forName(SENDER_CLASS);
        Constructor<?> constructor = senderClass.getConstructor(AgentConfig.class, boolean.class);
        assertNotNull(constructor, "PulsarMutationSender must expose an (AgentConfig, boolean) constructor");
    }

    @Test
    public void senderDoesNotExposeMessagingClientConstructor() throws Exception {
        Class<?> senderClass = Class.forName(SENDER_CLASS);
        // The previous factory reflected on this signature and failed at runtime with
        // NoSuchMethodException — guard against a regression to that broken contract.
        assertThrows(NoSuchMethodException.class,
                () -> senderClass.getConstructor(MessagingClient.class, boolean.class));
    }
}
