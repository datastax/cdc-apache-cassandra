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
package com.datastax.oss.cdc.messaging.pulsar;

import com.datastax.oss.cdc.messaging.schema.SchemaDefinition;
import com.datastax.oss.cdc.messaging.schema.SchemaException;
import com.datastax.oss.cdc.messaging.schema.SchemaInfo;
import com.datastax.oss.cdc.messaging.schema.impl.BaseSchemaProvider;
import org.apache.pulsar.client.api.PulsarClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pulsar-specific schema provider.
 * 
 * <p>Pulsar manages schemas automatically through its built-in schema registry.
 * This implementation extends BaseSchemaProvider for in-memory tracking while
 * Pulsar handles the actual schema storage and evolution.
 * 
 * <p>Schema registration happens automatically when producers/consumers are created
 * with schemas. This provider tracks schemas for validation and compatibility checking.
 */
public class PulsarSchemaProvider extends BaseSchemaProvider {
    
    private static final Logger log = LoggerFactory.getLogger(PulsarSchemaProvider.class);
    
    private final PulsarClient client;
    
    /**
     * Create a Pulsar schema provider.
     * 
     * @param client Pulsar client for schema operations
     */
    public PulsarSchemaProvider(PulsarClient client) {
        super();
        this.client = client;
        log.debug("PulsarSchemaProvider initialized");
    }
    
    @Override
    public SchemaInfo registerSchema(String topic, SchemaDefinition schema) throws SchemaException {
        log.debug("Registering schema for topic: {} (Pulsar auto-registers on producer/consumer creation)", topic);
        
        // Pulsar automatically registers schemas when producers/consumers are created
        // We use the base implementation for in-memory tracking
        return super.registerSchema(topic, schema);
    }
    
    /**
     * Get the Pulsar client.
     * 
     * @return Pulsar client
     */
    public PulsarClient getClient() {
        return client;
    }
}

// Made with Bob