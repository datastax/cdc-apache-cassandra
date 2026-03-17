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
package com.datastax.oss.cdc.messaging;

import java.io.Serializable;

/**
 * Abstraction for message identifier.
 * Platform-specific implementations provide unique message identification.
 * 
 * <p>Implementations must be:
 * <ul>
 *   <li>Serializable for persistence</li>
 *   <li>Comparable for ordering</li>
 *   <li>Immutable for thread safety</li>
 * </ul>
 */
public interface MessageId extends Serializable, Comparable<MessageId> {
    
    /**
     * Get byte array representation of message ID.
     * Used for serialization and storage.
     * 
     * @return Byte array representation
     */
    byte[] toByteArray();
    
    /**
     * Get string representation of message ID.
     * Used for logging and debugging.
     * 
     * @return String representation
     */
    @Override
    String toString();
    
    /**
     * Compare message IDs for ordering.
     * 
     * @param other Message ID to compare
     * @return Negative if this < other, 0 if equal, positive if this > other
     */
    @Override
    int compareTo(MessageId other);
    
    /**
     * Check equality with another message ID.
     * 
     * @param obj Object to compare
     * @return true if equal
     */
    @Override
    boolean equals(Object obj);
    
    /**
     * Get hash code for message ID.
     * 
     * @return Hash code
     */
    @Override
    int hashCode();
}

// Made with Bob
