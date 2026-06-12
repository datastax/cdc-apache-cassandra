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

/**
 * Schema information with version and metadata.
 * Returned when registering or retrieving schemas.
 */
public interface SchemaInfo {
    
    /**
     * Get schema definition.
     * 
     * @return SchemaDefinition
     */
    SchemaDefinition getSchema();
    
    /**
     * Get schema version.
     * Version number assigned by schema registry.
     * 
     * @return Schema version
     */
    int getVersion();
    
    /**
     * Get schema ID.
     * Unique identifier in schema registry.
     * 
     * @return Schema ID
     */
    String getSchemaId();
    
    /**
     * Get timestamp when schema was registered.
     * 
     * @return Timestamp in milliseconds since epoch
     */
    long getTimestamp();
}

