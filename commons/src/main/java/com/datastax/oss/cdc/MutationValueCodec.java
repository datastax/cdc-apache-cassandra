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

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * Canonical, provider-agnostic AVRO codec for {@link MutationValue}.
 *
 * <p>The Pulsar pipeline serializes {@link MutationValue} via Pulsar's reflection-based
 * {@code Schema.AVRO(MutationValue.class)}. The Kafka pipeline cannot rely on Pulsar types, so this
 * codec defines a single, explicit AVRO schema used symmetrically by the agent (producer side) and
 * the Kafka source connector (consumer side). Using an explicit schema (with {@code nodeId} encoded
 * as a string) avoids the fragility of AVRO reflection over {@link UUID} and keeps both ends in sync.
 *
 * <p>The schema is intentionally tolerant: every field is an optional union with a {@code null}
 * default so that schema evolution and absent values are handled gracefully.
 *
 * <p>This class is stateless and thread-safe.
 */
public final class MutationValueCodec {

    /**
     * Canonical AVRO schema for {@link MutationValue} used on the Kafka pipeline.
     */
    public static final Schema SCHEMA = SchemaBuilder.record("MutationValue")
            .namespace("com.datastax.oss.cdc")
            .fields()
            .name("md5Digest").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
            .name("nodeId").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
            .name("columns").type().unionOf().nullType().and().array().items().stringType().endUnion().nullDefault()
            .endRecord();

    private MutationValueCodec() {
        // utility class
    }

    /**
     * Convert a {@link MutationValue} into an AVRO {@link GenericRecord} matching {@link #SCHEMA}.
     *
     * @param mutationValue the value to convert (may be {@code null})
     * @return the corresponding generic record, or {@code null} if the input was {@code null}
     */
    public static GenericRecord toGenericRecord(MutationValue mutationValue) {
        if (mutationValue == null) {
            return null;
        }
        GenericData.Record record = new GenericData.Record(SCHEMA);
        record.put("md5Digest", mutationValue.getMd5Digest());
        record.put("nodeId", mutationValue.getNodeId() == null ? null : mutationValue.getNodeId().toString());
        record.put("columns", mutationValue.getColumns() == null
                ? null
                : new ArrayList<>(Arrays.asList(mutationValue.getColumns())));
        return record;
    }

    /**
     * Rebuild a {@link MutationValue} from an AVRO {@link GenericRecord} produced by
     * {@link #toGenericRecord(MutationValue)}.
     *
     * @param record the generic record (may be {@code null})
     * @return the reconstructed mutation value, or {@code null} if the input was {@code null}
     */
    public static MutationValue fromGenericRecord(GenericRecord record) {
        if (record == null) {
            return null;
        }
        String md5Digest = asString(record.get("md5Digest"));
        String nodeIdString = asString(record.get("nodeId"));
        UUID nodeId = nodeIdString == null ? null : UUID.fromString(nodeIdString);

        Object rawColumns = record.get("columns");
        String[] columns = null;
        if (rawColumns instanceof List) {
            List<?> list = (List<?>) rawColumns;
            columns = new String[list.size()];
            for (int i = 0; i < list.size(); i++) {
                Object element = list.get(i);
                columns[i] = element == null ? null : element.toString();
            }
        }
        return new MutationValue(md5Digest, nodeId, columns);
    }

    /**
     * Serialize a {@link MutationValue} to raw (registry-less) AVRO binary bytes using {@link #SCHEMA}.
     *
     * @param mutationValue the value to serialize (may be {@code null})
     * @return the AVRO-encoded bytes, or {@code null} if the input was {@code null}
     */
    public static byte[] serialize(MutationValue mutationValue) {
        if (mutationValue == null) {
            return null;
        }
        GenericRecord record = toGenericRecord(mutationValue);
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(SCHEMA);
            writer.write(record, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to serialize MutationValue", e);
        }
    }

    /**
     * Deserialize raw (registry-less) AVRO binary bytes (produced by {@link #serialize(MutationValue)})
     * back into a {@link MutationValue}.
     *
     * @param bytes the AVRO-encoded bytes (may be {@code null})
     * @return the reconstructed mutation value, or {@code null} if the input was {@code null}
     */
    public static MutationValue deserialize(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        try {
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
            DatumReader<GenericRecord> reader = new GenericDatumReader<>(SCHEMA);
            GenericRecord record = reader.read(null, decoder);
            return fromGenericRecord(record);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to deserialize MutationValue", e);
        }
    }

    private static String asString(Object value) {
        return value == null ? null : value.toString();
    }
}
