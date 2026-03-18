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
package com.datastax.oss.cdc.messaging.kafka;

import com.datastax.oss.cdc.messaging.schema.SchemaDefinition;
import com.datastax.oss.cdc.messaging.schema.SchemaInfo;
import com.datastax.oss.cdc.messaging.schema.SchemaType;
import com.datastax.oss.cdc.messaging.schema.impl.BaseSchemaInfo;
import com.datastax.oss.cdc.messaging.schema.impl.BaseSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka-specific schema provider using Confluent Schema Registry.
 * Handles AVRO schema registration and serialization/deserialization.
 * 
 * <p>Thread-safe.
 */
public class KafkaSchemaProvider extends BaseSchemaProvider {
    
    private static final Logger log = LoggerFactory.getLogger(KafkaSchemaProvider.class);
    
    private final SchemaRegistryClient schemaRegistry;
    private final KafkaAvroSerializer avroSerializer;
    private final KafkaAvroDeserializer avroDeserializer;
    private final String schemaRegistryUrl;
    
    /**
     * Create KafkaSchemaProvider with Schema Registry URL.
     */
    public KafkaSchemaProvider(String schemaRegistryUrl) {
        this(schemaRegistryUrl, new HashMap<>());
    }
    
    /**
     * Create KafkaSchemaProvider with Schema Registry URL and configuration.
     */
    public KafkaSchemaProvider(String schemaRegistryUrl, Map<String, Object> config) {
        this.schemaRegistryUrl = schemaRegistryUrl;
        
        // Create Schema Registry client
        this.schemaRegistry = new CachedSchemaRegistryClient(
            schemaRegistryUrl, 
            1000,  // max schemas to cache
            config
        );
        
        // Create serializer/deserializer
        Map<String, Object> serdeConfig = new HashMap<>(config);
        serdeConfig.put("schema.registry.url", schemaRegistryUrl);
        
        this.avroSerializer = new KafkaAvroSerializer(schemaRegistry, serdeConfig);
        this.avroDeserializer = new KafkaAvroDeserializer(schemaRegistry, serdeConfig);
        
        log.info("Initialized Kafka schema provider with registry: {}", schemaRegistryUrl);
    }
    
    @Override
    public SchemaInfo registerSchema(String topic, SchemaDefinition schemaDefinition) {
        try {
            String subject = topic + "-value";  // Kafka convention
            
            // Only support AVRO for now
            if (schemaDefinition.getType() != SchemaType.AVRO) {
                throw new UnsupportedOperationException(
                    "Only AVRO schemas are supported, got: " + schemaDefinition.getType());
            }
            
            // Parse AVRO schema
            Schema avroSchema = new Schema.Parser().parse(schemaDefinition.getSchemaDefinition());
            
            // Register with Schema Registry (using non-deprecated methods)
            @SuppressWarnings("deprecation")
            int schemaId = schemaRegistry.register(subject, avroSchema);
            @SuppressWarnings("deprecation")
            int version = schemaRegistry.getVersion(subject, avroSchema);
            
            // Create SchemaInfo
            SchemaInfo schemaInfo = BaseSchemaInfo.builder()
                .schema(schemaDefinition)
                .schemaId(String.valueOf(schemaId))
                .version(version)
                .timestamp(System.currentTimeMillis())
                .build();
            
            // Store in base provider's in-memory registry
            super.registerSchema(topic, schemaDefinition);
            
            log.info("Registered schema for topic {} with ID {} version {}", 
                    topic, schemaId, version);
            
            return schemaInfo;
            
        } catch (Exception e) {
            log.error("Failed to register schema for topic {}", topic, e);
            throw new RuntimeException("Failed to register schema", e);
        }
    }
    
    /**
     * Serialize an object using AVRO serialization.
     */
    @SuppressWarnings("unchecked")
    public <T> byte[] serialize(T data, String topic, boolean isKey) {
        try {
            if (data == null) {
                return null;
            }
            
            String subject = topic + (isKey ? "-key" : "-value");
            return avroSerializer.serialize(subject, data);
            
        } catch (Exception e) {
            log.error("Failed to serialize data for topic {}", topic, e);
            throw new RuntimeException("Failed to serialize data", e);
        }
    }
    
    /**
     * Deserialize bytes using AVRO deserialization.
     */
    @SuppressWarnings("unchecked")
    public <T> T deserialize(byte[] data, String topic, boolean isKey) {
        try {
            if (data == null) {
                return null;
            }
            
            String subject = topic + (isKey ? "-key" : "-value");
            return (T) avroDeserializer.deserialize(subject, data);
            
        } catch (Exception e) {
            log.error("Failed to deserialize data for topic {}", topic, e);
            throw new RuntimeException("Failed to deserialize data", e);
        }
    }
    
    /**
     * Get the Schema Registry client.
     */
    public SchemaRegistryClient getSchemaRegistry() {
        return schemaRegistry;
    }
    
    /**
     * Get the Schema Registry URL.
     */
    public String getSchemaRegistryUrl() {
        return schemaRegistryUrl;
    }
    
    /**
     * Get schema by ID from Schema Registry.
     */
    public Schema getSchemaById(int schemaId) {
        try {
            @SuppressWarnings("deprecation")
            Schema schema = schemaRegistry.getById(schemaId);
            return schema;
        } catch (Exception e) {
            log.error("Failed to get schema by ID {}", schemaId, e);
            throw new RuntimeException("Failed to get schema", e);
        }
    }
    
    /**
     * Get latest schema for a subject.
     */
    public Schema getLatestSchema(String subject) {
        try {
            String schemaString = schemaRegistry.getLatestSchemaMetadata(subject).getSchema();
            return new Schema.Parser().parse(schemaString);
        } catch (Exception e) {
            log.error("Failed to get latest schema for subject {}", subject, e);
            throw new RuntimeException("Failed to get schema", e);
        }
    }
    
    /**
     * Check if a schema is compatible with the latest version.
     */
    public boolean isCompatible(String subject, Schema schema) {
        try {
            @SuppressWarnings("deprecation")
            boolean compatible = schemaRegistry.testCompatibility(subject, schema);
            return compatible;
        } catch (Exception e) {
            log.error("Failed to check compatibility for subject {}", subject, e);
            return false;
        }
    }
    
    /**
     * Close the schema provider and release resources.
     */
    public void close() {
        try {
            avroSerializer.close();
            avroDeserializer.close();
            log.info("Closed Kafka schema provider");
        } catch (Exception e) {
            log.error("Error closing schema provider", e);
        }
    }
}

// Made with Bob
