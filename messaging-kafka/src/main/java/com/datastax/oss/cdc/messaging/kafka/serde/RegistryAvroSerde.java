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
package com.datastax.oss.cdc.messaging.kafka.serde;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Confluent Schema Registry-backed serde.
 *
 * <p>Uses {@link KafkaAvroSerializer}/{@link KafkaAvroDeserializer} configured against a Confluent
 * Schema Registry. Separate serializer/deserializer instances are configured for keys and values so
 * the correct {@code <topic>-key}/{@code <topic>-value} subject is derived by the default
 * {@code TopicNameStrategy} (passing the topic, not a pre-built subject, to the serializer).
 *
 * <p>Pre-serialized {@code byte[]} payloads are passed through unchanged so callers may opt to do
 * their own encoding; AVRO records (e.g. {@code GenericRecord}) are serialized and auto-registered.
 *
 * <p>Thread-safe.
 */
public class RegistryAvroSerde implements KafkaSerde {

    private static final Logger log = LoggerFactory.getLogger(RegistryAvroSerde.class);

    private final SchemaRegistryClient schemaRegistry;
    private final KafkaAvroSerializer keySerializer;
    private final KafkaAvroSerializer valueSerializer;
    private final KafkaAvroDeserializer keyDeserializer;
    private final KafkaAvroDeserializer valueDeserializer;
    private final String schemaRegistryUrl;

    public RegistryAvroSerde(String schemaRegistryUrl, Map<String, Object> providerProperties) {
        this.schemaRegistryUrl = schemaRegistryUrl;

        Map<String, Object> config = new HashMap<>();
        if (providerProperties != null) {
            providerProperties.forEach((k, v) -> config.put(k, v));
        }
        config.put("schema.registry.url", schemaRegistryUrl);

        this.schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryUrl, 1000, config);

        this.keySerializer = new KafkaAvroSerializer(schemaRegistry);
        this.keySerializer.configure(config, true);
        this.valueSerializer = new KafkaAvroSerializer(schemaRegistry);
        this.valueSerializer.configure(config, false);

        this.keyDeserializer = new KafkaAvroDeserializer(schemaRegistry);
        this.keyDeserializer.configure(config, true);
        this.valueDeserializer = new KafkaAvroDeserializer(schemaRegistry);
        this.valueDeserializer.configure(config, false);

        log.info("Initialized Confluent Schema Registry serde with registry: {}", schemaRegistryUrl);
    }

    @Override
    public byte[] serialize(Object data, String topic, boolean isKey) {
        if (data == null) {
            return null;
        }
        if (data instanceof byte[]) {
            return (byte[]) data;
        }
        return (isKey ? keySerializer : valueSerializer).serialize(topic, data);
    }

    @Override
    public Object deserialize(byte[] data, String topic, boolean isKey) {
        if (data == null) {
            return null;
        }
        return (isKey ? keyDeserializer : valueDeserializer).deserialize(topic, data);
    }

    public String getSchemaRegistryUrl() {
        return schemaRegistryUrl;
    }

    public SchemaRegistryClient getSchemaRegistry() {
        return schemaRegistry;
    }

    @Override
    public void close() {
        try {
            keySerializer.close();
            valueSerializer.close();
            keyDeserializer.close();
            valueDeserializer.close();
        } catch (Exception e) {
            log.warn("Error closing Confluent serde", e);
        }
    }
}
