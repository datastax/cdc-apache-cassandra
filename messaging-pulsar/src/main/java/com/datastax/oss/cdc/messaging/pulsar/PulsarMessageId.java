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

import com.datastax.oss.cdc.messaging.MessageId;

import java.util.Arrays;

/**
 * Pulsar implementation of MessageId.
 * Wraps Apache Pulsar MessageId.
 * 
 * <p>Immutable and thread-safe.
 */
public class PulsarMessageId implements MessageId {
    
    private static final long serialVersionUID = 1L;
    
    private final org.apache.pulsar.client.api.MessageId pulsarMessageId;
    private final byte[] idBytes;
    private final int hashCode;
    
    /**
     * Create message ID from Pulsar message ID.
     * 
     * @param pulsarMessageId Pulsar message ID
     */
    public PulsarMessageId(org.apache.pulsar.client.api.MessageId pulsarMessageId) {
        if (pulsarMessageId == null) {
            throw new IllegalArgumentException("Pulsar message ID cannot be null");
        }
        
        this.pulsarMessageId = pulsarMessageId;
        this.idBytes = pulsarMessageId.toByteArray();
        this.hashCode = Arrays.hashCode(this.idBytes);
    }
    
    @Override
    public byte[] toByteArray() {
        // Return defensive copy to maintain immutability
        return Arrays.copyOf(idBytes, idBytes.length);
    }
    
    @Override
    public String toString() {
        return pulsarMessageId.toString();
    }
    
    @Override
    public int compareTo(MessageId other) {
        if (other == null) {
            return 1;
        }
        
        if (other instanceof PulsarMessageId) {
            PulsarMessageId otherPulsar = (PulsarMessageId) other;
            // Use Pulsar's native comparison
            return pulsarMessageId.compareTo(otherPulsar.pulsarMessageId);
        }
        
        // Fall back to byte array comparison for cross-implementation compatibility
        return Arrays.compare(this.idBytes, other.toByteArray());
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        PulsarMessageId other = (PulsarMessageId) obj;
        return pulsarMessageId.equals(other.pulsarMessageId);
    }
    
    @Override
    public int hashCode() {
        return hashCode;
    }
    
    /**
     * Get underlying Pulsar message ID.
     * For internal use only.
     * 
     * @return Pulsar message ID instance
     */
    public org.apache.pulsar.client.api.MessageId getPulsarMessageId() {
        return pulsarMessageId;
    }
}

// Made with Bob
