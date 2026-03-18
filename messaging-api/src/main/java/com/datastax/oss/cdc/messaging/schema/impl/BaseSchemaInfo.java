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
package com.datastax.oss.cdc.messaging.schema.impl;

import com.datastax.oss.cdc.messaging.schema.SchemaDefinition;
import com.datastax.oss.cdc.messaging.schema.SchemaInfo;

import java.util.Objects;

/**
 * Base implementation of SchemaInfo.
 * Immutable and thread-safe.
 */
public class BaseSchemaInfo implements SchemaInfo {
    
    private final SchemaDefinition schema;
    private final int version;
    private final String schemaId;
    private final long timestamp;
    
    /**
     * Create schema info.
     * 
     * @param schema Schema definition
     * @param version Schema version
     * @param schemaId Schema ID
     * @param timestamp Registration timestamp
     */
    public BaseSchemaInfo(
            SchemaDefinition schema,
            int version,
            String schemaId,
            long timestamp) {
        
        this.schema = Objects.requireNonNull(schema, "Schema cannot be null");
        this.version = version;
        this.schemaId = Objects.requireNonNull(schemaId, "Schema ID cannot be null");
        this.timestamp = timestamp;
    }
    
    @Override
    public SchemaDefinition getSchema() {
        return schema;
    }
    
    @Override
    public int getVersion() {
        return version;
    }
    
    @Override
    public String getSchemaId() {
        return schemaId;
    }
    
    @Override
    public long getTimestamp() {
        return timestamp;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        
        BaseSchemaInfo that = (BaseSchemaInfo) o;
        return version == that.version &&
               schemaId.equals(that.schemaId) &&
               schema.equals(that.schema);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(schema, version, schemaId);
    }
    
    @Override
    public String toString() {
        return String.format("BaseSchemaInfo{schemaId=%s, version=%d, schemaName=%s, timestamp=%d}",
            schemaId, version, schema.getName(), timestamp);
    }
    
    /**
     * Create a builder for BaseSchemaInfo.
     * 
     * @return Builder instance
     */
    public static Builder builder() {
        return new Builder();
    }
    
    /**
     * Builder for BaseSchemaInfo.
     */
    public static class Builder {
        private SchemaDefinition schema;
        private int version;
        private String schemaId;
        private long timestamp = System.currentTimeMillis();
        
        /**
         * Set schema definition.
         * 
         * @param schema Schema definition
         * @return This builder
         */
        public Builder schema(SchemaDefinition schema) {
            this.schema = schema;
            return this;
        }
        
        /**
         * Set schema version.
         * 
         * @param version Schema version
         * @return This builder
         */
        public Builder version(int version) {
            this.version = version;
            return this;
        }
        
        /**
         * Set schema ID.
         * 
         * @param schemaId Schema ID
         * @return This builder
         */
        public Builder schemaId(String schemaId) {
            this.schemaId = schemaId;
            return this;
        }
        
        /**
         * Set registration timestamp.
         * 
         * @param timestamp Timestamp in milliseconds
         * @return This builder
         */
        public Builder timestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }
        
        /**
         * Build the schema info.
         * 
         * @return BaseSchemaInfo instance
         * @throws IllegalStateException if required fields not set
         */
        public BaseSchemaInfo build() {
            if (schema == null) {
                throw new IllegalStateException("Schema is required");
            }
            if (schemaId == null) {
                throw new IllegalStateException("Schema ID is required");
            }
            if (version < 0) {
                throw new IllegalStateException("Schema version must be non-negative");
            }
            
            return new BaseSchemaInfo(schema, version, schemaId, timestamp);
        }
    }
}

// Made with Bob