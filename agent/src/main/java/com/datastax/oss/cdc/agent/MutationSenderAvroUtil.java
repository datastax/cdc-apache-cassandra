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
import com.datastax.oss.cdc.agent.exceptions.CassandraConnectorSchemaException;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Shared Avro utilities for mutation senders.
 * Has no dependency on Pulsar or Kafka — safe to use from both
 * {@code AbstractPulsarMutationSender} and {@code AbstractKafkaMutationSender}.
 */
public final class MutationSenderAvroUtil {

    public static final String SCHEMA_DOC_PREFIX = "Primary key schema for table ";

    static {
        // Register CQL logical-type conversions once for the whole JVM.
        SpecificData.get().addLogicalTypeConversion(new CqlLogicalTypes.CqlVarintConversion());
        SpecificData.get().addLogicalTypeConversion(new CqlLogicalTypes.CqlDecimalConversion());
        SpecificData.get().addLogicalTypeConversion(new Conversions.UUIDConversion());
    }

    private MutationSenderAvroUtil() {
    }

    /**
     * Pairs an Avro {@link Schema} with the {@link SpecificDatumWriter} built for it.
     */
    @AllArgsConstructor
    @ToString
    @EqualsAndHashCode
    public static class SchemaAndWriter {
        public final Schema schema;
        public final SpecificDatumWriter<GenericRecord> writer;
    }

    /**
     * Serialises an Avro {@link GenericRecord} to a binary byte array using the supplied writer.
     */
    public static byte[] serializeAvroGenericRecord(
            GenericRecord genericRecord,
            SpecificDatumWriter<GenericRecord> datumWriter) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = new EncoderFactory().binaryEncoder(out, null);
            datumWriter.write(genericRecord, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Builds (and caches in {@code pkSchemas}) the Avro primary-key schema for the given table.
     *
     * @param tableInfo   table metadata
     * @param pkSchemas   per-sender cache; pass the sender's own map so entries are reused
     * @param nativeSchema function that maps a CQL3 type string to the corresponding Avro schema
     * @return the schema and its writer
     */
    public static SchemaAndWriter getAvroKeySchema(
            TableInfo tableInfo,
            Map<String, SchemaAndWriter> pkSchemas,
            java.util.function.Function<String, Schema> nativeSchema) {
        return pkSchemas.computeIfAbsent(tableInfo.key(), k -> {
            List<Schema.Field> fields = new ArrayList<>();
            for (ColumnInfo cm : tableInfo.primaryKeyColumns()) {
                Schema.Field field = new Schema.Field(cm.name(), nativeSchema.apply(cm.cql3Type()));
                if (cm.isClusteringKey()) {
                    // clustering keys are optional
                    field = new Schema.Field(cm.name(),
                            org.apache.avro.SchemaBuilder.unionOf().nullType().and().type(field.schema()).endUnion());
                }
                fields.add(field);
            }
            Schema avroSchema = Schema.createRecord(
                    tableInfo.key(), SCHEMA_DOC_PREFIX + tableInfo.key(), tableInfo.name(), false, fields);
            return new SchemaAndWriter(avroSchema, new SpecificDatumWriter<>(avroSchema));
        });
    }

    /**
     * Builds an Avro {@link GenericRecord} containing the mutation's primary-key values.
     *
     * @param keySchema  the schema returned by {@link #getAvroKeySchema}
     * @param mutation   the mutation whose PK values to encode
     * @param cqlToAvro  converts a raw CQL value to the matching Avro type;
     *                   receives the mutation metadata, column name, and raw value
     */
    public static <T> GenericRecord buildAvroKey(
            Schema keySchema,
            AbstractMutation<T> mutation,
            BiFunction<String, Object, Object> cqlToAvro) {
        GenericRecord record = new org.apache.avro.generic.GenericData.Record(keySchema);
        int i = 0;
        for (ColumnInfo columnInfo : mutation.primaryKeyColumns()) {
            if (keySchema.getField(columnInfo.name()) == null) {
                throw new CassandraConnectorSchemaException("Not a valid schema field: " + columnInfo.name());
            }
            record.put(columnInfo.name(), cqlToAvro.apply(columnInfo.name(), mutation.getPkValues()[i++]));
        }
        return record;
    }
}
