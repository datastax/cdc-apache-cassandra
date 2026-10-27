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
package com.datastax.oss.cdc.messaging.util;

import com.datastax.oss.cdc.messaging.schema.SchemaDefinition;
import com.datastax.oss.cdc.messaging.schema.SchemaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Schema handling utilities.
 * Provides helper methods for schema validation and manipulation.
 * 
 * <p>Thread-safe utility class with static methods.
 */
public final class SchemaUtils {
    
    private static final Logger log = LoggerFactory.getLogger(SchemaUtils.class);
    
    private SchemaUtils() {
        // Utility class
    }
    
    /**
     * Validate schema definition.
     * Checks for required fields and format correctness.
     * 
     * @param schema Schema to validate
     * @throws IllegalArgumentException if schema invalid
     */
    public static void validateSchema(SchemaDefinition schema) {
        if (schema == null) {
            throw new IllegalArgumentException("Schema cannot be null");
        }
        
        if (schema.getType() == null) {
            throw new IllegalArgumentException("Schema type cannot be null");
        }
        
        String definition = schema.getSchemaDefinition();
        if (definition == null || definition.trim().isEmpty()) {
            throw new IllegalArgumentException("Schema definition cannot be null or empty");
        }
        
        String name = schema.getName();
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("Schema name cannot be null or empty");
        }
        
        // Type-specific validation
        switch (schema.getType()) {
            case AVRO:
                validateAvroSchema(definition);
                break;
            case JSON:
                validateJsonSchema(definition);
                break;
            case PROTOBUF:
                validateProtobufSchema(definition);
                break;
            case BYTES:
            case STRING:
                // No additional validation needed
                break;
            default:
                log.warn("Unknown schema type: {}", schema.getType());
        }
        
        log.debug("Schema validation passed for: {}", name);
    }
    
    /**
     * Validate AVRO schema format.
     * 
     * @param definition Schema definition string
     */
    private static void validateAvroSchema(String definition) {
        // Basic JSON structure check
        if (!definition.trim().startsWith("{") && !definition.trim().startsWith("[")) {
            throw new IllegalArgumentException("AVRO schema must be valid JSON");
        }
        
        // Check for required AVRO fields
        if (!definition.contains("\"type\"")) {
            throw new IllegalArgumentException("AVRO schema must contain 'type' field");
        }
    }
    
    /**
     * Validate JSON schema format.
     * 
     * @param definition Schema definition string
     */
    private static void validateJsonSchema(String definition) {
        // Basic JSON structure check
        if (!definition.trim().startsWith("{")) {
            throw new IllegalArgumentException("JSON schema must be valid JSON object");
        }
    }
    
    /**
     * Validate Protobuf schema format.
     * 
     * @param definition Schema definition string
     */
    private static void validateProtobufSchema(String definition) {
        // Basic protobuf syntax check
        if (!definition.contains("message") && !definition.contains("enum")) {
            throw new IllegalArgumentException(
                "Protobuf schema must contain 'message' or 'enum' definition");
        }
    }
    
    /**
     * Check if schema is valid without throwing exception.
     * 
     * @param schema Schema to check
     * @return true if valid, false otherwise
     */
    public static boolean isValidSchema(SchemaDefinition schema) {
        try {
            validateSchema(schema);
            return true;
        } catch (IllegalArgumentException e) {
            log.debug("Schema validation failed: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * Check basic compatibility between two schemas.
     * This is a simplified check; platform-specific providers should implement
     * full compatibility checking.
     * 
     * @param schema1 First schema
     * @param schema2 Second schema
     * @return true if schemas appear compatible
     */
    public static boolean areCompatible(SchemaDefinition schema1, SchemaDefinition schema2) {
        if (schema1 == null || schema2 == null) {
            return false;
        }
        
        // Must be same type
        if (schema1.getType() != schema2.getType()) {
            log.debug("Schema types differ: {} vs {}", schema1.getType(), schema2.getType());
            return false;
        }
        
        // Same name indicates same schema family
        if (!schema1.getName().equals(schema2.getName())) {
            log.debug("Schema names differ: {} vs {}", schema1.getName(), schema2.getName());
            return false;
        }
        
        // Delegate to schema's own compatibility check
        return schema1.isCompatibleWith(schema2);
    }
    
    /**
     * Get schema type from definition string.
     * Attempts to detect schema type from content.
     * 
     * @param definition Schema definition string
     * @return Detected SchemaType or null if cannot determine
     */
    public static SchemaType detectSchemaType(String definition) {
        if (definition == null || definition.trim().isEmpty()) {
            return null;
        }
        
        String trimmed = definition.trim();
        
        // Check for AVRO (JSON with type field)
        if (trimmed.startsWith("{") && definition.contains("\"type\"")) {
            return SchemaType.AVRO;
        }
        
        // Check for JSON schema
        if (trimmed.startsWith("{") && definition.contains("\"$schema\"")) {
            return SchemaType.JSON;
        }
        
        // Check for Protobuf
        if (definition.contains("syntax = \"proto") || 
            (definition.contains("message") && definition.contains("{"))) {
            return SchemaType.PROTOBUF;
        }
        
        // Default to STRING for simple text
        return SchemaType.STRING;
    }
    
    /**
     * Compare schema definitions for equality.
     * Normalizes whitespace before comparison.
     * 
     * @param def1 First definition
     * @param def2 Second definition
     * @return true if definitions are equivalent
     */
    public static boolean areDefinitionsEqual(String def1, String def2) {
        if (def1 == null && def2 == null) {
            return true;
        }
        if (def1 == null || def2 == null) {
            return false;
        }
        
        // Normalize whitespace for comparison
        String normalized1 = def1.replaceAll("\\s+", " ").trim();
        String normalized2 = def2.replaceAll("\\s+", " ").trim();
        
        return normalized1.equals(normalized2);
    }
    
    /**
     * Extract schema name from definition if not explicitly provided.
     * 
     * @param definition Schema definition
     * @param type Schema type
     * @return Extracted name or "anonymous" if cannot extract
     */
    public static String extractSchemaName(String definition, SchemaType type) {
        if (definition == null || type == null) {
            return "anonymous";
        }
        
        switch (type) {
            case AVRO:
                return extractAvroName(definition);
            case PROTOBUF:
                return extractProtobufName(definition);
            default:
                return "anonymous";
        }
    }
    
    /**
     * Extract name from AVRO schema.
     * 
     * @param definition AVRO schema definition
     * @return Schema name or "anonymous"
     */
    private static String extractAvroName(String definition) {
        // Simple regex to find "name" field
        int nameIndex = definition.indexOf("\"name\"");
        if (nameIndex == -1) {
            return "anonymous";
        }
        
        int colonIndex = definition.indexOf(":", nameIndex);
        if (colonIndex == -1) {
            return "anonymous";
        }
        
        int startQuote = definition.indexOf("\"", colonIndex);
        if (startQuote == -1) {
            return "anonymous";
        }
        
        int endQuote = definition.indexOf("\"", startQuote + 1);
        if (endQuote == -1) {
            return "anonymous";
        }
        
        return definition.substring(startQuote + 1, endQuote);
    }
    
    /**
     * Extract name from Protobuf schema.
     * 
     * @param definition Protobuf schema definition
     * @return Schema name or "anonymous"
     */
    private static String extractProtobufName(String definition) {
        // Find first message definition
        int messageIndex = definition.indexOf("message");
        if (messageIndex == -1) {
            return "anonymous";
        }
        
        int nameStart = messageIndex + 7; // "message".length()
        while (nameStart < definition.length() && 
               Character.isWhitespace(definition.charAt(nameStart))) {
            nameStart++;
        }
        
        int nameEnd = nameStart;
        while (nameEnd < definition.length() && 
               (Character.isLetterOrDigit(definition.charAt(nameEnd)) || 
                definition.charAt(nameEnd) == '_')) {
            nameEnd++;
        }
        
        if (nameEnd > nameStart) {
            return definition.substring(nameStart, nameEnd);
        }
        
        return "anonymous";
    }
    
    /**
     * Log schema details for debugging.
     * 
     * @param schema Schema to log
     * @param prefix Log prefix
     */
    public static void logSchema(SchemaDefinition schema, String prefix) {
        if (schema == null) {
            log.debug("{}: null schema", prefix);
            return;
        }
        
        log.debug("{}: name={}, type={}, properties={}, definitionLength={}",
            prefix,
            schema.getName(),
            schema.getType(),
            schema.getProperties().size(),
            schema.getSchemaDefinition().length());
    }
}

