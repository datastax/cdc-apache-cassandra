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

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RawAvroSerdeTest {

    private final RawAvroSerde serde = new RawAvroSerde();

    private static final Schema SCHEMA = SchemaBuilder.record("Key").fields()
            .requiredString("id").endRecord();

    @Test
    public void shouldPassThroughByteArrays() {
        byte[] bytes = {1, 2, 3, 4};
        // identity (same reference) for pre-serialized payloads
        assertSame(bytes, serde.serialize(bytes, "topic", true));
    }

    @Test
    public void shouldReturnNullForNull() {
        assertNull(serde.serialize(null, "topic", false));
    }

    @Test
    public void shouldEncodeAvroRecordWithItsOwnSchema() throws Exception {
        GenericData.Record record = new GenericData.Record(SCHEMA);
        record.put("id", "hello");

        byte[] encoded = serde.serialize(record, "topic", false);

        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(SCHEMA);
        GenericRecord decoded = reader.read(null,
                DecoderFactory.get().binaryDecoder(encoded, null));
        assertEquals("hello", decoded.get("id").toString());
    }

    @Test
    public void shouldEncodeCharSequenceAsUtf8() {
        assertArrayEquals("abc".getBytes(StandardCharsets.UTF_8), serde.serialize("abc", "t", true));
    }

    @Test
    public void shouldRejectUnsupportedTypes() {
        assertThrows(IllegalArgumentException.class, () -> serde.serialize(42, "t", false));
    }

    @Test
    public void deserializeShouldReturnRawBytesForCallerToDecode() {
        byte[] bytes = {9, 8, 7};
        assertSame(bytes, serde.deserialize(bytes, "t", false));
        assertNull(serde.deserialize(null, "t", false));
    }
}
