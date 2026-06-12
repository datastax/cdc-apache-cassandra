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

import lombok.extern.slf4j.Slf4j;

/**
 * @deprecated Use {@link AbstractMessagingMutationSender} instead.
 * This class is maintained for backward compatibility only.
 * It now extends AbstractMessagingMutationSender and delegates all functionality
 * to the messaging abstraction layer, removing direct Pulsar client dependencies.
 * 
 * <p>Migration from direct Pulsar APIs to messaging abstraction layer:
 * <ul>
 *   <li>PulsarClient → MessagingClient (via MessagingClientFactory)</li>
 *   <li>Producer<KeyValue> → MessageProducer<byte[], MutationValue></li>
 *   <li>Direct Pulsar configuration → Provider-agnostic ClientConfig</li>
 *   <li>Pulsar-specific schemas → SchemaDefinition abstraction</li>
 * </ul>
 * 
 * <p>All concrete implementations (C3, C4, DSE4) already extend AbstractMessagingMutationSender
 * directly, so this class serves only as a compatibility layer for external extensions.
 * 
 * <p>This class will be removed in a future release.
 */
@Deprecated
@Slf4j
public abstract class AbstractPulsarMutationSender<T> extends AbstractMessagingMutationSender<T> {

    /**
     * Constructor that delegates to parent AbstractMessagingMutationSender.
     * All Pulsar-specific initialization is now handled through the messaging
     * abstraction layer.
     * 
     * @param config Agent configuration containing messaging provider settings
     * @param useMurmur3Partitioner Whether to use Murmur3 partitioning for message routing
     */
    public AbstractPulsarMutationSender(AgentConfig config, boolean useMurmur3Partitioner) {
        super(config, useMurmur3Partitioner);
        log.warn("AbstractPulsarMutationSender is deprecated. Please migrate to AbstractMessagingMutationSender " +
                "to use the provider-agnostic messaging abstraction layer.");
    }
}
