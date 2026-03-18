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
import com.datastax.oss.cdc.messaging.schema.SchemaType;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Base implementation of SchemaDefinition.
 * Immutable and thread-safe.
 */
public class BaseSchemaDefinition implements SchemaDefinition {
    
    private final SchemaType type;
    private final String schemaDefinition;
    private final Map<String, String> properties;
    private final Object nativeSchema;
    private final String name;
    
    /**
     * Create schema definition.
     * 
     * @param type Schema type
     * @param schemaDefinition Schema definition string
     * @param properties Schema properties
     * @param nativeSchema Native schema object
     * @param name Schema name
     */
    public BaseSchemaDefinition(
            SchemaType type,
            String schemaDefinition,
            Map<String, String> properties,
            Object nativeSchema,
            String name) {
        
        this.type = Objects.requireNonNull(type, "Schema type cannot be null");
        this.schemaDefinition = Objects.requireNonNull(schemaDefinition, 
            "Schema definition cannot be null");
        this.properties = properties != null ? 
            Collections.unmodifiableMap(new HashMap<>(properties)) : 
            Collections.emptyMap();
        this.nativeSchema = nativeSchema;
        this.name = Objects.requireNonNull(name, "Schema name cannot be null");
    }
    
    @Override
    public SchemaType getType() {
        return type;
    }
    
    @Override
    public String getSchemaDefinition() {
        return schemaDefinition;
    }
    
    @Override
    public Map<String, String> getProperties() {
        return properties;
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public <T> T getNativeSchema() {
        return (T) nativeSchema;
    }
    
    @Override
    public String getName() {
        return name;
    }
    
    @Override
    public boolean isCompatibleWith(SchemaDefinition other) {
        if (other == null) {
            return false;
        }
        
        // Must be same type
        if (this.type != other.getType()) {
            return false;
        }
        
        // Same name indicates same schema family
        if (!this.name.equals(other.getName())) {
            return false;
        }
        
        // For simple types, definition must match exactly
        if (type == SchemaType.STRING || type == SchemaType.BYTES) {
            return this.schemaDefinition.equals(other.getSchemaDefinition());
        }
        
        // For complex types, delegate to platform-specific logic
        // This is a basic check; implementations should override for full compatibility
        return true;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        
        BaseSchemaDefinition that = (BaseSchemaDefinition) o;
        return type == that.type &&
               name.equals(that.name) &&
               schemaDefinition.equals(that.schemaDefinition);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(type, name, schemaDefinition);
    }
    
    @Override
    public String toString() {
        return String.format("BaseSchemaDefinition{type=%s, name=%s, definitionLength=%d, properties=%d}",
            type, name, schemaDefinition.length(), properties.size());
    }
    
    /**
     * Create a builder for BaseSchemaDefinition.
     * 
     * @return Builder instance
     */
    public static Builder builder() {
        return new Builder();
    }
    
    /**
     * Builder for BaseSchemaDefinition.
     */
    public static class Builder {
        private SchemaType type;
        private String schemaDefinition;
        private Map<String, String> properties = new HashMap<>();
        private Object nativeSchema;
        private String name;
        
        /**
         * Set schema type.
         * 
         * @param type Schema type
         * @return This builder
         */
        public Builder type(SchemaType type) {
            this.type = type;
            return this;
        }
        
        /**
         * Set schema definition string.
         * 
         * @param schemaDefinition Schema definition
         * @return This builder
         */
        public Builder schemaDefinition(String schemaDefinition) {
            this.schemaDefinition = schemaDefinition;
            return this;
        }
        
        /**
         * Set schema properties.
         * 
         * @param properties Properties map
         * @return This builder
         */
        public Builder properties(Map<String, String> properties) {
            if (properties != null) {
                this.properties = new HashMap<>(properties);
            }
            return this;
        }
        
        /**
         * Add a single property.
         * 
         * @param key Property key
         * @param value Property value
         * @return This builder
         */
        public Builder property(String key, String value) {
            this.properties.put(key, value);
            return this;
        }
        
        /**
         * Set native schema object.
         * 
         * @param nativeSchema Native schema
         * @return This builder
         */
        public Builder nativeSchema(Object nativeSchema) {
            this.nativeSchema = nativeSchema;
            return this;
        }
        
        /**
         * Set schema name.
         * 
         * @param name Schema name
         * @return This builder
         */
        public Builder name(String name) {
            this.name = name;
            return this;
        }
        
        /**
         * Build the schema definition.
         * 
         * @return BaseSchemaDefinition instance
         * @throws IllegalStateException if required fields not set
         */
        public BaseSchemaDefinition build() {
            if (type == null) {
                throw new IllegalStateException("Schema type is required");
            }
            if (schemaDefinition == null) {
                throw new IllegalStateException("Schema definition is required");
            }
            if (name == null) {
                throw new IllegalStateException("Schema name is required");
            }
            
            return new BaseSchemaDefinition(type, schemaDefinition, properties, nativeSchema, name);
        }
    }
}

// Made with Bob