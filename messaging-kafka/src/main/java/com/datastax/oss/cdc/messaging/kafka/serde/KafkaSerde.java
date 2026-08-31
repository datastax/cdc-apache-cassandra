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

/**
 * Strategy for serializing/deserializing Kafka record keys and values.
 *
 * <p>Two implementations are provided:
 * <ul>
 *   <li>{@link RawAvroSerde} &mdash; registry-less: AVRO records are encoded to raw binary using
 *       their own schema, pre-serialized {@code byte[]} payloads pass through unchanged. Works with
 *       plain Apache Kafka with no Schema Registry.</li>
 *   <li>{@link RegistryAvroSerde} &mdash; integrates with a Confluent Schema Registry via
 *       {@code KafkaAvroSerializer}/{@code KafkaAvroDeserializer}.</li>
 * </ul>
 *
 * <p>The provider is selected by {@code com.datastax.oss.cdc.messaging.kafka.KafkaMessagingClient}
 * based on whether a {@code schema.registry.url} is configured. Implementations must be thread-safe.
 */
public interface KafkaSerde extends AutoCloseable {

    /**
     * Serialize an object to the {@code byte[]} payload Kafka expects.
     *
     * @param data  the key or value to serialize (may be {@code null})
     * @param topic the Kafka topic (used to derive the registry subject)
     * @param isKey {@code true} when serializing a key, {@code false} for a value
     * @return the serialized bytes, or {@code null} if {@code data} was {@code null}
     */
    byte[] serialize(Object data, String topic, boolean isKey);

    /**
     * Deserialize a {@code byte[]} payload received from Kafka.
     *
     * <p>The registry-less implementation returns the bytes unchanged (the caller owns decoding,
     * since raw AVRO binary carries no embedded schema); the registry implementation returns the
     * decoded object.
     *
     * @param data  the raw bytes (may be {@code null})
     * @param topic the Kafka topic (used to derive the registry subject)
     * @param isKey {@code true} when deserializing a key, {@code false} for a value
     * @return the deserialized object, or {@code null} if {@code data} was {@code null}
     */
    Object deserialize(byte[] data, String topic, boolean isKey);

    @Override
    default void close() {
        // no-op by default
    }
}
