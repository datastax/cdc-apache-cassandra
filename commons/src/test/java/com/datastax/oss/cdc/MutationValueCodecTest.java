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
package com.datastax.oss.cdc;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Unit tests for the registry-less {@link MutationValueCodec} used on the Kafka pipeline.
 */
public class MutationValueCodecTest {

    @Test
    public void shouldRoundTripFullValue() {
        UUID nodeId = UUID.randomUUID();
        MutationValue original = new MutationValue("md5abc", nodeId, new String[]{"col1", "col2"});

        MutationValue decoded = MutationValueCodec.deserialize(MutationValueCodec.serialize(original));

        assertEquals("md5abc", decoded.getMd5Digest());
        assertEquals(nodeId, decoded.getNodeId());
        assertArrayEquals(new String[]{"col1", "col2"}, decoded.getColumns());
    }

    @Test
    public void shouldRoundTripWithNullColumns() {
        UUID nodeId = UUID.randomUUID();
        MutationValue original = new MutationValue("digest", nodeId, null);

        MutationValue decoded = MutationValueCodec.deserialize(MutationValueCodec.serialize(original));

        assertEquals("digest", decoded.getMd5Digest());
        assertEquals(nodeId, decoded.getNodeId());
        assertNull(decoded.getColumns());
    }

    @Test
    public void shouldRoundTripWithEmptyColumns() {
        MutationValue original = new MutationValue("d", UUID.randomUUID(), new String[]{});

        MutationValue decoded = MutationValueCodec.deserialize(MutationValueCodec.serialize(original));

        assertArrayEquals(new String[]{}, decoded.getColumns());
    }

    @Test
    public void shouldRoundTripWithNullNodeId() {
        MutationValue original = new MutationValue("d", null, new String[]{"c"});

        MutationValue decoded = MutationValueCodec.deserialize(MutationValueCodec.serialize(original));

        assertNull(decoded.getNodeId());
        assertEquals("d", decoded.getMd5Digest());
    }

    @Test
    public void shouldHandleNulls() {
        assertNull(MutationValueCodec.serialize(null));
        assertNull(MutationValueCodec.deserialize(null));
        assertNull(MutationValueCodec.toGenericRecord(null));
        assertNull(MutationValueCodec.fromGenericRecord(null));
    }

    @Test
    public void shouldRoundTripViaGenericRecord() {
        UUID nodeId = UUID.randomUUID();
        MutationValue original = new MutationValue("digest", nodeId, new String[]{"a"});

        GenericRecord record = MutationValueCodec.toGenericRecord(original);
        assertEquals(MutationValueCodec.SCHEMA, record.getSchema());

        MutationValue decoded = MutationValueCodec.fromGenericRecord(record);
        assertEquals(original.getMd5Digest(), decoded.getMd5Digest());
        assertEquals(original.getNodeId(), decoded.getNodeId());
        assertArrayEquals(original.getColumns(), decoded.getColumns());
    }

    @Test
    public void serializationShouldBeDeterministic() {
        MutationValue value = new MutationValue("d", new UUID(1L, 2L), new String[]{"x"});
        assertArrayEquals(MutationValueCodec.serialize(value), MutationValueCodec.serialize(value));
    }
}
