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

import java.util.Optional;

/**
 * Abstraction for schema management.
 * Handles schema registration, retrieval, and evolution.
 * 
 * <p>Implementations interact with platform-specific schema registries
 * (Pulsar Schema Registry, Confluent Schema Registry, etc.).
 */
public interface SchemaProvider {
    
    /**
     * Register a schema for a topic.
     * If schema already exists and is compatible, returns existing version.
     * 
     * @param topic Topic name
     * @param schema Schema definition
     * @return SchemaInfo with version and ID
     * @throws SchemaException if registration fails or schema incompatible
     */
    SchemaInfo registerSchema(String topic, SchemaDefinition schema) 
        throws SchemaException;
    
    /**
     * Get latest schema for topic.
     * 
     * @param topic Topic name
     * @return SchemaInfo or empty if no schema registered
     * @throws SchemaException if retrieval fails
     */
    Optional<SchemaInfo> getSchema(String topic) throws SchemaException;
    
    /**
     * Get specific schema version for topic.
     * 
     * @param topic Topic name
     * @param version Schema version
     * @return SchemaInfo or empty if version not found
     * @throws SchemaException if retrieval fails
     */
    Optional<SchemaInfo> getSchema(String topic, int version) throws SchemaException;
    
    /**
     * Check if new schema is compatible with existing schemas.
     * Uses platform-specific compatibility rules.
     * 
     * @param topic Topic name
     * @param schema New schema to check
     * @return true if compatible with existing schemas
     * @throws SchemaException if compatibility check fails
     */
    boolean isCompatible(String topic, SchemaDefinition schema) throws SchemaException;
    
    /**
     * Delete schema for topic.
     * May not be supported by all platforms.
     * 
     * @param topic Topic name
     * @throws SchemaException if deletion fails or not supported
     */
    void deleteSchema(String topic) throws SchemaException;
    
    /**
     * Get all schema versions for topic.
     * 
     * @param topic Topic name
     * @return Array of schema versions
     * @throws SchemaException if retrieval fails
     */
    int[] getVersions(String topic) throws SchemaException;
}

// Made with Bob
