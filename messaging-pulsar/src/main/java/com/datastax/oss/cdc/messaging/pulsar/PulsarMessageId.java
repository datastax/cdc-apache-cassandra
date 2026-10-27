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
package com.datastax.oss.cdc.messaging.pulsar;

import com.datastax.oss.cdc.messaging.impl.BaseMessageId;

/**
 * Pulsar-specific implementation of MessageId.
 * Wraps Pulsar's native MessageId and provides access to it for Pulsar-specific operations.
 * 
 * <p>Thread-safe and immutable.
 */
public class PulsarMessageId extends BaseMessageId {
    
    private static final long serialVersionUID = 1L;
    
    private final org.apache.pulsar.client.api.MessageId pulsarMessageId;
    
    /**
     * Create PulsarMessageId from Pulsar's native MessageId.
     * 
     * @param pulsarMessageId Pulsar MessageId instance
     * @throws IllegalArgumentException if pulsarMessageId is null
     */
    public PulsarMessageId(org.apache.pulsar.client.api.MessageId pulsarMessageId) {
        super(pulsarMessageId.toByteArray());
        if (pulsarMessageId == null) {
            throw new IllegalArgumentException("Pulsar MessageId cannot be null");
        }
        this.pulsarMessageId = pulsarMessageId;
    }
    
    /**
     * Get the underlying Pulsar MessageId.
     * Used for Pulsar-specific operations like acknowledgment.
     * 
     * @return Pulsar MessageId instance
     */
    public org.apache.pulsar.client.api.MessageId getPulsarMessageId() {
        return pulsarMessageId;
    }
    
    @Override
    public String toString() {
        return pulsarMessageId.toString();
    }
}

