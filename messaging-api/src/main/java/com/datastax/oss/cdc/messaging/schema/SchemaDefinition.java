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
package com.datastax.oss.cdc.messaging.schema;

import java.util.Map;

/**
 * Abstraction for schema definition.
 * Platform-agnostic schema representation.
 * 
 * <p>Implementations should be immutable and thread-safe.
 */
public interface SchemaDefinition {
    
    /**
     * Get schema type.
     * 
     * @return SchemaType (AVRO, JSON, PROTOBUF, etc.)
     */
    SchemaType getType();
    
    /**
     * Get schema as string representation.
     * Format depends on schema type:
     * <ul>
     *   <li>AVRO: JSON schema definition</li>
     *   <li>JSON: JSON schema definition</li>
     *   <li>PROTOBUF: Proto file content</li>
     * </ul>
     * 
     * @return Schema definition string
     */
    String getSchemaDefinition();
    
    /**
     * Get schema properties (metadata).
     * 
     * @return Immutable map of properties
     */
    Map<String, String> getProperties();
    
    /**
     * Get native schema object (platform-specific).
     * Returns the underlying platform-specific schema representation.
     * 
     * @param <T> Native schema type
     * @return Native schema object
     */
    <T> T getNativeSchema();
    
    /**
     * Get schema name.
     * 
     * @return Schema name or empty for anonymous schemas
     */
    String getName();
    
    /**
     * Check if schema is compatible with another schema.
     * 
     * @param other Schema to compare
     * @return true if compatible
     */
    boolean isCompatibleWith(SchemaDefinition other);
}

// Made with Bob
