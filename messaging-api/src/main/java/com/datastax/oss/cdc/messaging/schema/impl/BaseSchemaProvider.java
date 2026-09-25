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
import com.datastax.oss.cdc.messaging.schema.SchemaException;
import com.datastax.oss.cdc.messaging.schema.SchemaInfo;
import com.datastax.oss.cdc.messaging.schema.SchemaProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base implementation of SchemaProvider.
 * Provides in-memory schema registry with version management.
 * 
 * <p>Thread-safe implementation suitable for testing and simple use cases.
 * Production implementations should use platform-specific schema registries.
 */
public class BaseSchemaProvider implements SchemaProvider {
    
    private static final Logger log = LoggerFactory.getLogger(BaseSchemaProvider.class);
    
    // Schema storage: topic -> version -> SchemaInfo
    private final Map<String, Map<Integer, SchemaInfo>> schemaRegistry = new ConcurrentHashMap<>();
    
    // Version counters per topic
    private final Map<String, AtomicInteger> versionCounters = new ConcurrentHashMap<>();
    
    /**
     * Create a new base schema provider.
     */
    public BaseSchemaProvider() {
        log.debug("BaseSchemaProvider initialized");
    }
    
    @Override
    public SchemaInfo registerSchema(String topic, SchemaDefinition schema) throws SchemaException {
        if (topic == null || topic.trim().isEmpty()) {
            throw new SchemaException("Topic cannot be null or empty");
        }
        if (schema == null) {
            throw new SchemaException("Schema cannot be null");
        }
        
        log.debug("Registering schema for topic: {}, schemaName: {}", topic, schema.getName());
        
        // Get or create version counter for topic
        AtomicInteger versionCounter = versionCounters.computeIfAbsent(
            topic, k -> new AtomicInteger(0));
        
        // Get or create schema map for topic
        Map<Integer, SchemaInfo> topicSchemas = schemaRegistry.computeIfAbsent(
            topic, k -> new ConcurrentHashMap<>());
        
        // Check if schema already exists
        Optional<SchemaInfo> existing = findExistingSchema(topicSchemas, schema);
        if (existing.isPresent()) {
            log.debug("Schema already registered for topic: {}, version: {}", 
                topic, existing.get().getVersion());
            return existing.get();
        }
        
        // Check compatibility with latest version
        if (!topicSchemas.isEmpty()) {
            int latestVersion = versionCounter.get();
            SchemaInfo latestSchema = topicSchemas.get(latestVersion);
            if (latestSchema != null && !isCompatible(topic, schema)) {
                throw new SchemaException(
                    String.format("Schema incompatible with existing schemas for topic: %s", topic));
            }
        }
        
        // Register new version
        int newVersion = versionCounter.incrementAndGet();
        String schemaId = generateSchemaId(topic, newVersion);
        
        SchemaInfo schemaInfo = BaseSchemaInfo.builder()
            .schema(schema)
            .version(newVersion)
            .schemaId(schemaId)
            .timestamp(System.currentTimeMillis())
            .build();
        
        topicSchemas.put(newVersion, schemaInfo);
        
        log.info("Registered schema for topic: {}, version: {}, schemaId: {}", 
            topic, newVersion, schemaId);
        
        return schemaInfo;
    }
    
    @Override
    public Optional<SchemaInfo> getSchema(String topic) throws SchemaException {
        if (topic == null || topic.trim().isEmpty()) {
            throw new SchemaException("Topic cannot be null or empty");
        }
        
        Map<Integer, SchemaInfo> topicSchemas = schemaRegistry.get(topic);
        if (topicSchemas == null || topicSchemas.isEmpty()) {
            log.debug("No schema found for topic: {}", topic);
            return Optional.empty();
        }
        
        // Get latest version
        AtomicInteger versionCounter = versionCounters.get(topic);
        if (versionCounter == null) {
            return Optional.empty();
        }
        
        int latestVersion = versionCounter.get();
        SchemaInfo schemaInfo = topicSchemas.get(latestVersion);
        
        log.debug("Retrieved latest schema for topic: {}, version: {}", topic, latestVersion);
        return Optional.ofNullable(schemaInfo);
    }
    
    @Override
    public Optional<SchemaInfo> getSchema(String topic, int version) throws SchemaException {
        if (topic == null || topic.trim().isEmpty()) {
            throw new SchemaException("Topic cannot be null or empty");
        }
        if (version < 0) {
            throw new SchemaException("Version must be non-negative");
        }
        
        Map<Integer, SchemaInfo> topicSchemas = schemaRegistry.get(topic);
        if (topicSchemas == null) {
            log.debug("No schema found for topic: {}", topic);
            return Optional.empty();
        }
        
        SchemaInfo schemaInfo = topicSchemas.get(version);
        log.debug("Retrieved schema for topic: {}, version: {}, found: {}", 
            topic, version, schemaInfo != null);
        
        return Optional.ofNullable(schemaInfo);
    }
    
    @Override
    public boolean isCompatible(String topic, SchemaDefinition schema) throws SchemaException {
        if (topic == null || topic.trim().isEmpty()) {
            throw new SchemaException("Topic cannot be null or empty");
        }
        if (schema == null) {
            throw new SchemaException("Schema cannot be null");
        }
        
        Map<Integer, SchemaInfo> topicSchemas = schemaRegistry.get(topic);
        if (topicSchemas == null || topicSchemas.isEmpty()) {
            // No existing schemas, so compatible
            return true;
        }
        
        // Check compatibility with all existing versions
        for (SchemaInfo existingSchema : topicSchemas.values()) {
            if (!schema.isCompatibleWith(existingSchema.getSchema())) {
                log.debug("Schema incompatible with version: {}", existingSchema.getVersion());
                return false;
            }
        }
        
        log.debug("Schema compatible with all existing versions for topic: {}", topic);
        return true;
    }
    
    @Override
    public void deleteSchema(String topic) throws SchemaException {
        if (topic == null || topic.trim().isEmpty()) {
            throw new SchemaException("Topic cannot be null or empty");
        }
        
        Map<Integer, SchemaInfo> removed = schemaRegistry.remove(topic);
        versionCounters.remove(topic);
        
        if (removed != null) {
            log.info("Deleted all schemas for topic: {}, versions: {}", topic, removed.size());
        } else {
            log.debug("No schemas to delete for topic: {}", topic);
        }
    }
    
    @Override
    public int[] getVersions(String topic) throws SchemaException {
        if (topic == null || topic.trim().isEmpty()) {
            throw new SchemaException("Topic cannot be null or empty");
        }
        
        Map<Integer, SchemaInfo> topicSchemas = schemaRegistry.get(topic);
        if (topicSchemas == null || topicSchemas.isEmpty()) {
            return new int[0];
        }
        
        List<Integer> versions = new ArrayList<>(topicSchemas.keySet());
        versions.sort(Integer::compareTo);
        
        int[] result = new int[versions.size()];
        for (int i = 0; i < versions.size(); i++) {
            result[i] = versions.get(i);
        }
        
        log.debug("Retrieved {} versions for topic: {}", result.length, topic);
        return result;
    }
    
    /**
     * Find existing schema that matches the given schema.
     * 
     * @param topicSchemas Schemas for topic
     * @param schema Schema to find
     * @return Existing schema info if found
     */
    private Optional<SchemaInfo> findExistingSchema(
            Map<Integer, SchemaInfo> topicSchemas,
            SchemaDefinition schema) {
        
        for (SchemaInfo existing : topicSchemas.values()) {
            if (schemasEqual(existing.getSchema(), schema)) {
                return Optional.of(existing);
            }
        }
        return Optional.empty();
    }
    
    /**
     * Check if two schemas are equal.
     * 
     * @param schema1 First schema
     * @param schema2 Second schema
     * @return true if equal
     */
    private boolean schemasEqual(SchemaDefinition schema1, SchemaDefinition schema2) {
        return schema1.getType() == schema2.getType() &&
               schema1.getName().equals(schema2.getName()) &&
               schema1.getSchemaDefinition().equals(schema2.getSchemaDefinition());
    }
    
    /**
     * Generate unique schema ID.
     * 
     * @param topic Topic name
     * @param version Schema version
     * @return Schema ID
     */
    private String generateSchemaId(String topic, int version) {
        return String.format("%s-v%d-%d", topic, version, System.currentTimeMillis());
    }
    
    /**
     * Get total number of registered schemas across all topics.
     * 
     * @return Total schema count
     */
    public int getTotalSchemaCount() {
        return schemaRegistry.values().stream()
            .mapToInt(Map::size)
            .sum();
    }
    
    /**
     * Get number of topics with registered schemas.
     * 
     * @return Topic count
     */
    public int getTopicCount() {
        return schemaRegistry.size();
    }
    
    /**
     * Clear all schemas (for testing).
     */
    public void clear() {
        schemaRegistry.clear();
        versionCounters.clear();
        log.debug("Cleared all schemas from registry");
    }
}

