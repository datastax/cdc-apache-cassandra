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
package com.datastax.oss.cdc.messaging.impl;

import com.datastax.oss.cdc.messaging.MessageId;

import java.util.Arrays;
import java.util.Objects;

/**
 * Base implementation of MessageId.
 * Provides immutable message identifier with byte array representation.
 * 
 * <p>Thread-safe and immutable.
 */
public class BaseMessageId implements MessageId {
    
    private static final long serialVersionUID = 1L;
    
    private final byte[] idBytes;
    private final int hashCode;
    
    /**
     * Create message ID from byte array.
     * 
     * @param idBytes Byte array representation (copied internally)
     * @throws IllegalArgumentException if idBytes is null or empty
     */
    public BaseMessageId(byte[] idBytes) {
        if (idBytes == null || idBytes.length == 0) {
            throw new IllegalArgumentException("Message ID bytes cannot be null or empty");
        }
        // Defensive copy for immutability
        this.idBytes = Arrays.copyOf(idBytes, idBytes.length);
        this.hashCode = Arrays.hashCode(this.idBytes);
    }
    
    /**
     * Create message ID from string.
     * String is converted to UTF-8 bytes.
     * 
     * @param id String representation
     * @throws IllegalArgumentException if id is null or empty
     */
    public BaseMessageId(String id) {
        if (id == null || id.isEmpty()) {
            throw new IllegalArgumentException("Message ID string cannot be null or empty");
        }
        this.idBytes = id.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        this.hashCode = Arrays.hashCode(this.idBytes);
    }
    
    @Override
    public byte[] toByteArray() {
        // Return defensive copy to maintain immutability
        return Arrays.copyOf(idBytes, idBytes.length);
    }
    
    @Override
    public String toString() {
        return new String(idBytes, java.nio.charset.StandardCharsets.UTF_8);
    }
    
    @Override
    public int compareTo(MessageId other) {
        if (other == null) {
            return 1;
        }
        if (!(other instanceof BaseMessageId)) {
            // Compare by string representation for cross-implementation compatibility
            return toString().compareTo(other.toString());
        }
        
        BaseMessageId otherBase = (BaseMessageId) other;
        return Arrays.compare(this.idBytes, otherBase.idBytes);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        BaseMessageId other = (BaseMessageId) obj;
        return Arrays.equals(idBytes, other.idBytes);
    }
    
    @Override
    public int hashCode() {
        return hashCode;
    }
}

