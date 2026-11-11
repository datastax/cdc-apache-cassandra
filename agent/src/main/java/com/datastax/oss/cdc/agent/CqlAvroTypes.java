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
import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;

import java.net.InetAddress;
import java.time.Instant;
import java.util.Date;
import java.util.Map;

/**
 * Maps primary key columns to Avro, keyed by CQL type name (for example {@code text} or
 * {@code timestamp}). The names are the same on Cassandra 3.11, 4.0 and DSE 6.8, and a reversed
 * clustering column reports its base type, so this needs no Cassandra classes and is shared by
 * every agent flavor.
 */
public final class CqlAvroTypes {

    private static final String TIMESTAMP = "timestamp";
    private static final String DATE = "date";
    private static final String TIME = "time";
    private static final String INET = "inet";
    private static final String TINYINT = "tinyint";
    private static final String SMALLINT = "smallint";

    private static final Map<String, Schema> NATIVE_SCHEMAS = ImmutableMap.<String, Schema>builder()
            .put("text", Schema.create(Schema.Type.STRING))
            .put("ascii", Schema.create(Schema.Type.STRING))
            .put("boolean", Schema.create(Schema.Type.BOOLEAN))
            .put("blob", Schema.create(Schema.Type.BYTES))
            .put(TINYINT, Schema.create(Schema.Type.INT))
            .put(SMALLINT, Schema.create(Schema.Type.INT))
            .put("int", Schema.create(Schema.Type.INT))
            .put("varint", CqlLogicalTypes.varintType)
            .put("bigint", Schema.create(Schema.Type.LONG))
            .put("float", Schema.create(Schema.Type.FLOAT))
            .put("double", Schema.create(Schema.Type.DOUBLE))
            .put("decimal", CqlLogicalTypes.decimalType)
            .put(INET, Schema.create(Schema.Type.STRING))
            .put(TIMESTAMP, CqlLogicalTypes.timestampMillisType)
            .put(DATE, CqlLogicalTypes.dateType)
            .put(TIME, CqlLogicalTypes.timeMicrosType)
            .put("duration", CqlLogicalTypes.durationType)
            .put("uuid", CqlLogicalTypes.uuidType)
            .put("timeuuid", CqlLogicalTypes.uuidType)
            .build();

    private CqlAvroTypes() {
    }

    /** Avro schema for a CQL type name, or {@code null} if it can't be a Kafka key column. */
    public static Schema nativeSchema(String cql3Type) {
        return NATIVE_SCHEMAS.get(cql3Type);
    }

    public static boolean isSupported(String cql3Type) {
        return NATIVE_SCHEMAS.containsKey(cql3Type);
    }

    /** Converts a deserialized primary key value of the given CQL type to its Avro form. */
    public static Object toAvro(String cql3Type, Object value) {
        if (value == null) {
            return null;
        }
        switch (cql3Type) {
            case TIMESTAMP:
                if (value instanceof Date) {
                    return ((Date) value).getTime();
                }
                if (value instanceof Instant) {
                    return ((Instant) value).toEpochMilli();
                }
                return value;
            case DATE:
                // Cassandra stores dates as an unsigned day count centered on 2^31.
                return value instanceof Integer
                        ? (int) (Integer.toUnsignedLong((Integer) value) - (1L << 31))
                        : value;
            case TIME:
                // Nanoseconds since midnight to the microseconds of Avro time-micros.
                return value instanceof Long ? (Long) value / 1000 : value;
            case INET:
                return ((InetAddress) value).getHostAddress();
            case TINYINT:
                return Byte.toUnsignedInt((byte) value);
            case SMALLINT:
                return Short.toUnsignedInt((short) value);
            default:
                return value;
        }
    }
}
