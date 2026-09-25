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

import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

/**
 * Registry-less serde that encodes AVRO records to raw binary using their own schema.
 *
 * <p>Serialization rules:
 * <ul>
 *   <li>{@code null} &rarr; {@code null}</li>
 *   <li>{@code byte[]} &rarr; returned unchanged (already serialized, e.g. an AVRO-encoded key)</li>
 *   <li>{@link GenericContainer} (AVRO {@code GenericRecord}/{@code SpecificRecord}) &rarr; raw AVRO
 *       binary encoded with the record's own schema</li>
 *   <li>{@link CharSequence} &rarr; UTF-8 bytes</li>
 * </ul>
 *
 * <p>Deserialization returns the raw bytes unchanged: raw AVRO binary carries no embedded schema, so
 * decoding is the caller's responsibility (the consumer knows the reader schema from domain context).
 *
 * <p>Stateless and thread-safe.
 */
public class RawAvroSerde implements KafkaSerde {

    @Override
    public byte[] serialize(Object data, String topic, boolean isKey) {
        if (data == null) {
            return null;
        }
        if (data instanceof byte[]) {
            return (byte[]) data;
        }
        if (data instanceof GenericContainer) {
            return encodeAvro((GenericContainer) data);
        }
        if (data instanceof CharSequence) {
            return data.toString().getBytes(StandardCharsets.UTF_8);
        }
        throw new IllegalArgumentException(
                "RawAvroSerde cannot serialize type " + data.getClass().getName()
                        + "; expected byte[], an AVRO GenericContainer, or a CharSequence");
    }

    @Override
    public Object deserialize(byte[] data, String topic, boolean isKey) {
        // Raw AVRO binary has no embedded schema; hand the bytes back to the caller to decode.
        return data;
    }

    private static byte[] encodeAvro(GenericContainer container) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            DatumWriter<Object> writer = new GenericDatumWriter<>(container.getSchema());
            writer.write(container, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to AVRO-encode record", e);
        }
    }
}
