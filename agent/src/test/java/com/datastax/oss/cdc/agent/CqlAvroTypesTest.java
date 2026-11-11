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
package com.datastax.oss.cdc.agent;

import com.datastax.oss.cdc.CqlLogicalTypes;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.time.Instant;
import java.util.Date;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CqlAvroTypesTest {

    @Test
    void mapsEveryKeyTypeTheAgentSupports() {
        assertEquals(Schema.Type.STRING, CqlAvroTypes.nativeSchema("text").getType());
        assertEquals(Schema.Type.STRING, CqlAvroTypes.nativeSchema("ascii").getType());
        assertEquals(Schema.Type.BOOLEAN, CqlAvroTypes.nativeSchema("boolean").getType());
        assertEquals(Schema.Type.BYTES, CqlAvroTypes.nativeSchema("blob").getType());
        assertEquals(Schema.Type.INT, CqlAvroTypes.nativeSchema("tinyint").getType());
        assertEquals(Schema.Type.INT, CqlAvroTypes.nativeSchema("smallint").getType());
        assertEquals(Schema.Type.INT, CqlAvroTypes.nativeSchema("int").getType());
        assertEquals(Schema.Type.LONG, CqlAvroTypes.nativeSchema("bigint").getType());
        assertEquals(Schema.Type.FLOAT, CqlAvroTypes.nativeSchema("float").getType());
        assertEquals(Schema.Type.DOUBLE, CqlAvroTypes.nativeSchema("double").getType());
        assertEquals(Schema.Type.STRING, CqlAvroTypes.nativeSchema("inet").getType());
        assertSame(CqlLogicalTypes.varintType, CqlAvroTypes.nativeSchema("varint"));
        assertSame(CqlLogicalTypes.decimalType, CqlAvroTypes.nativeSchema("decimal"));
        assertSame(CqlLogicalTypes.timestampMillisType, CqlAvroTypes.nativeSchema("timestamp"));
        assertSame(CqlLogicalTypes.dateType, CqlAvroTypes.nativeSchema("date"));
        assertSame(CqlLogicalTypes.timeMicrosType, CqlAvroTypes.nativeSchema("time"));
        assertSame(CqlLogicalTypes.durationType, CqlAvroTypes.nativeSchema("duration"));
        assertSame(CqlLogicalTypes.uuidType, CqlAvroTypes.nativeSchema("uuid"));
        assertSame(CqlLogicalTypes.uuidType, CqlAvroTypes.nativeSchema("timeuuid"));
    }

    @Test
    void rejectsCollectionsAndUdts() {
        assertFalse(CqlAvroTypes.isSupported("list<int>"));
        assertFalse(CqlAvroTypes.isSupported("frozen<address>"));
        assertNull(CqlAvroTypes.nativeSchema("list<int>"));
        assertTrue(CqlAvroTypes.isSupported("timeuuid"));
    }

    @Test
    void convertsTimestamps() {
        assertEquals(1234L, CqlAvroTypes.toAvro("timestamp", new Date(1234L)));
        assertEquals(5678L, CqlAvroTypes.toAvro("timestamp", Instant.ofEpochMilli(5678L)));
    }

    @Test
    void convertsUnsignedDayCountToEpochDay() {
        assertEquals(0, CqlAvroTypes.toAvro("date", Integer.MIN_VALUE));
        assertEquals(19000, CqlAvroTypes.toAvro("date", Integer.MIN_VALUE + 19000));
        assertEquals(Integer.MIN_VALUE, CqlAvroTypes.toAvro("date", 0));
        assertEquals(-1, CqlAvroTypes.toAvro("date", Integer.MAX_VALUE));
    }

    @Test
    void convertsTimeNanosToMicros() {
        assertEquals(3_000L, CqlAvroTypes.toAvro("time", 3_000_999L));
    }

    @Test
    void convertsInetAndSmallIntegers() throws Exception {
        assertEquals("10.0.0.1", CqlAvroTypes.toAvro("inet", InetAddress.getByName("10.0.0.1")));
        assertEquals(255, CqlAvroTypes.toAvro("tinyint", (byte) -1));
        assertEquals(65535, CqlAvroTypes.toAvro("smallint", (short) -1));
    }

    @Test
    void passesOtherValuesThrough() {
        UUID id = UUID.randomUUID();
        assertSame(id, CqlAvroTypes.toAvro("uuid", id));
        assertEquals("abc", CqlAvroTypes.toAvro("text", "abc"));
        assertNull(CqlAvroTypes.toAvro("text", null));
    }
}
