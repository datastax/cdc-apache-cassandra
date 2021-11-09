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
package com.datastax.oss.pulsar.source.converters;

import com.datastax.oss.cdc.AvroSchemaWrapper;
import com.datastax.oss.cdc.CqlLogicalTypes;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.pulsar.source.Converter;
import com.google.common.base.Preconditions;
import com.google.common.net.InetAddresses;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.Schema.*;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;

/**
 * AVRO Converter providing support for logical types.
 */
@Slf4j
public class NativeAvroConverter implements Converter<byte[], GenericRecord, Row, List<Object>> {

    public final org.apache.pulsar.client.api.Schema<byte[]> pulsarSchema;
    public final Schema avroSchema;
    public final TableMetadata tableMetadata;
    public final Map<String, Schema> udtSchemas = new HashMap<>();

    public NativeAvroConverter(KeyspaceMetadata ksm, TableMetadata tm, List<ColumnMetadata> columns) {
        this.tableMetadata = tm;
        String keyspaceAndTable = ksm.getName() + "." + tm.getName();
        List<Schema.Field> fields = new ArrayList<>();
        for(ColumnMetadata cm : columns) {
            boolean isPartitionKey = tm.getPartitionKey().contains(cm);
            if (isSupportedCqlType(cm.getType())) {
                Schema.Field field = fieldSchema(ksm, cm.getName().toString(), cm.getType(), !isPartitionKey);
                if (field != null)
                    fields.add(field);
            }
        }
        this.avroSchema = Schema.createRecord(keyspaceAndTable, "Table " + keyspaceAndTable, ksm.getName().asInternal(), false, fields);
        this.pulsarSchema = new AvroSchemaWrapper(avroSchema);
        if (log.isInfoEnabled()) {
            log.info("schema={}", this.avroSchema);
            for(Map.Entry<String, Schema> entry : udtSchemas.entrySet()) {
                log.info("type={} schema={}", entry.getKey(), entry.getValue());
            }
        }
    }

    Schema.Field fieldSchema(KeyspaceMetadata ksm,
                       String fieldName,
                       DataType dataType,
                       boolean optional) {
        Schema.Field fieldSchema = null;
        switch(dataType.getProtocolCode()) {
            case ProtocolConstants.DataType.INET:
            case ProtocolConstants.DataType.ASCII:
            case ProtocolConstants.DataType.VARCHAR:
                fieldSchema = new Schema.Field(fieldName, org.apache.avro.Schema.create(Type.STRING));
                break;
            case ProtocolConstants.DataType.BLOB:
                fieldSchema = new Schema.Field(fieldName, org.apache.avro.Schema.create(Type.BYTES));
                break;
            case ProtocolConstants.DataType.TINYINT:
            case ProtocolConstants.DataType.SMALLINT:
            case ProtocolConstants.DataType.INT:
                fieldSchema = new Schema.Field(fieldName, org.apache.avro.Schema.create(Type.INT));
                break;
            case ProtocolConstants.DataType.BIGINT:
                fieldSchema = new Schema.Field(fieldName, org.apache.avro.Schema.create(Type.LONG));
                break;
            case ProtocolConstants.DataType.BOOLEAN:
                fieldSchema = new Schema.Field(fieldName, org.apache.avro.Schema.create(Type.BOOLEAN));
                break;
            case ProtocolConstants.DataType.FLOAT:
                fieldSchema = new Schema.Field(fieldName, org.apache.avro.Schema.create(Type.FLOAT));
                break;
            case ProtocolConstants.DataType.DOUBLE:
                fieldSchema = new Schema.Field(fieldName, org.apache.avro.Schema.create(Type.DOUBLE));
                break;
            case ProtocolConstants.DataType.TIMESTAMP:
                fieldSchema = new Schema.Field(fieldName, CqlLogicalTypes.timestampMillisType);
                break;
            case ProtocolConstants.DataType.DATE:
                fieldSchema = new Schema.Field(fieldName, CqlLogicalTypes.dateType);
                break;
            case ProtocolConstants.DataType.TIME:
                fieldSchema = new Schema.Field(fieldName, CqlLogicalTypes.timeMicrosType);
                break;
            case ProtocolConstants.DataType.UDT:
                fieldSchema = new Schema.Field(fieldName, buildUDTSchema(ksm, dataType.asCql(false, true), optional));
                break;
            case ProtocolConstants.DataType.UUID:
            case ProtocolConstants.DataType.TIMEUUID:
                fieldSchema = new Schema.Field(fieldName, CqlLogicalTypes.uuidType);
                break;
            case ProtocolConstants.DataType.VARINT:
                fieldSchema = new Schema.Field(fieldName, CqlLogicalTypes.varintType);
                break;
            case ProtocolConstants.DataType.DECIMAL:
                fieldSchema = new Schema.Field(fieldName, CqlLogicalTypes.decimalType);
                break;
            case ProtocolConstants.DataType.DURATION:
                fieldSchema = new Schema.Field(fieldName, CqlLogicalTypes.durationType);
                break;
            default:
                log.debug("Ignoring unsupported type fields name={} type={}", fieldName, dataType.asCql(false, true));
                return null;
        }

        if (optional) {
            fieldSchema = new Schema.Field(fieldName, SchemaBuilder.unionOf().nullType().and().type(fieldSchema.schema()).endUnion(), null, Field.NULL_DEFAULT_VALUE);
        }
        return fieldSchema;
    }

    Schema buildUDTSchema(KeyspaceMetadata ksm, String typeName, boolean optional) {
        UserDefinedType userDefinedType = ksm.getUserDefinedType(CqlIdentifier.fromCql(typeName.substring(typeName.indexOf(".") + 1)))
                .orElseThrow(() -> new IllegalStateException("UDT " + typeName + " not found"));
        List<Schema.Field> fieldSchemas = new ArrayList<>();
        int i = 0;
        for(CqlIdentifier field : userDefinedType.getFieldNames()) {
            Schema.Field fieldSchema = fieldSchema(ksm, field.toString(), userDefinedType.getFieldTypes().get(i++), optional);
            if (fieldSchema != null)
                fieldSchemas.add(fieldSchema);
        }
        Schema udtSchema = Schema.createRecord(typeName, "CQL type " + typeName, ksm.getName().asInternal(), false, fieldSchemas);
        udtSchemas.put(typeName, udtSchema);
        return udtSchema;
    }

    @Override
    public org.apache.pulsar.client.api.Schema<byte[]> getSchema() {
        return this.pulsarSchema;
    }

    @Override
    public byte[] toConnectData(Row row) {
        GenericRecord genericRecordBuilder = new GenericData.Record(avroSchema);
        for(ColumnDefinition cm : row.getColumnDefinitions()) {
            if (!row.isNull(cm.getName())) {
                switch (cm.getType().getProtocolCode()) {
                    case ProtocolConstants.DataType.UUID:
                    case ProtocolConstants.DataType.TIMEUUID:
                        genericRecordBuilder.put(cm.getName().toString(), row.getUuid(cm.getName()));
                        break;
                    case ProtocolConstants.DataType.ASCII:
                    case ProtocolConstants.DataType.VARCHAR:
                        genericRecordBuilder.put(cm.getName().toString(), row.getString(cm.getName()));
                        break;
                    case ProtocolConstants.DataType.TINYINT:
                        genericRecordBuilder.put(cm.getName().toString(), (int) row.getByte(cm.getName()));
                        break;
                    case ProtocolConstants.DataType.SMALLINT:
                        genericRecordBuilder.put(cm.getName().toString(), (int) row.getShort(cm.getName()));
                        break;
                    case ProtocolConstants.DataType.INT:
                        genericRecordBuilder.put(cm.getName().toString(), row.getInt(cm.getName()));
                        break;
                    case ProtocolConstants.DataType.BIGINT:
                        genericRecordBuilder.put(cm.getName().toString(), row.getLong(cm.getName()));
                        break;
                    case ProtocolConstants.DataType.INET:
                        genericRecordBuilder.put(cm.getName().toString(), row.getInetAddress(cm.getName()).getHostAddress());
                        break;
                    case ProtocolConstants.DataType.DOUBLE:
                        genericRecordBuilder.put(cm.getName().toString(), row.getDouble(cm.getName()));
                        break;
                    case ProtocolConstants.DataType.FLOAT:
                        genericRecordBuilder.put(cm.getName().toString(), row.getFloat(cm.getName()));
                        break;
                    case ProtocolConstants.DataType.BOOLEAN:
                        genericRecordBuilder.put(cm.getName().toString(), row.getBoolean(cm.getName()));
                        break;
                    case ProtocolConstants.DataType.TIMESTAMP:
                        genericRecordBuilder.put(cm.getName().toString(), row.getInstant(cm.getName()).toEpochMilli());
                        break;
                    case ProtocolConstants.DataType.DATE: // Avro date is epoch days
                        genericRecordBuilder.put(cm.getName().toString(), (int) row.getLocalDate(cm.getName()).toEpochDay());
                        break;
                    case ProtocolConstants.DataType.TIME: // Avro time is epoch milliseconds
                        genericRecordBuilder.put(cm.getName().toString(), (row.getLocalTime(cm.getName()).toNanoOfDay() / 1000));
                        break;
                    case ProtocolConstants.DataType.BLOB:
                        genericRecordBuilder.put(cm.getName().toString(), row.getByteBuffer(cm.getName()));
                        break;
                    case ProtocolConstants.DataType.UDT:
                        genericRecordBuilder.put(cm.getName().toString(), buildUDTValue(row.getUdtValue(cm.getName())));
                        break;
                    case ProtocolConstants.DataType.DURATION:
                        genericRecordBuilder.put(cm.getName().toString(), row.getCqlDuration(cm.getName()));
                        break;
                    case ProtocolConstants.DataType.DECIMAL:
                        genericRecordBuilder.put(cm.getName().toString(), row.getBigDecimal(cm.getName()));
                        break;
                    case ProtocolConstants.DataType.VARINT:
                        genericRecordBuilder.put(cm.getName().toString(), row.getBigInteger(cm.getName()));
                        break;
                    default:
                        log.debug("Ignoring unsupported column name={} type={}", cm.getName(), cm.getType().asCql(false, true));
                }
            }
        }
        return serializeAvroGenericRecord(genericRecordBuilder, avroSchema);
    }

    public static byte[] serializeAvroGenericRecord(org.apache.avro.generic.GenericRecord genericRecord, org.apache.avro.Schema schema) {
        try {
            SpecificDatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<>(schema);
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            BinaryEncoder binaryEncoder = new EncoderFactory().binaryEncoder(byteArrayOutputStream, null);
            datumWriter.write(genericRecord, binaryEncoder);
            binaryEncoder.flush();
            return byteArrayOutputStream.toByteArray();
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    GenericRecord buildUDTValue(UdtValue udtValue) {
        String typeName = udtValue.getType().getKeyspace() + "." + udtValue.getType().getName();
        Schema udtSchema = udtSchemas.get(typeName);
        Preconditions.checkNotNull(udtSchema, "Schema not found for UDT=" + typeName);
        Preconditions.checkState(udtSchema.getFields().size() > 0, "Schema UDT=" + typeName + " has no fields");

        List<String> fields = udtSchema.getFields().stream().map(Field::name).collect(Collectors.toList());
        GenericRecord genericRecord = new GenericData.Record(udtSchema);
        for(CqlIdentifier field : udtValue.getType().getFieldNames()) {
            if (fields.contains(field.asInternal()) && !udtValue.isNull(field)) {
                DataType dataType = udtValue.getType(field);
                switch (dataType.getProtocolCode()) {
                    case ProtocolConstants.DataType.UUID:
                    case ProtocolConstants.DataType.TIMEUUID:
                        genericRecord.put(field.toString(), udtValue.getUuid(field));
                        break;
                    case ProtocolConstants.DataType.ASCII:
                    case ProtocolConstants.DataType.VARCHAR:
                        genericRecord.put(field.toString(), udtValue.getString(field));
                        break;
                    case ProtocolConstants.DataType.TINYINT:
                        genericRecord.put(field.toString(), (int) udtValue.getByte(field));
                        break;
                    case ProtocolConstants.DataType.SMALLINT:
                        genericRecord.put(field.toString(), (int) udtValue.getShort(field));
                        break;
                    case ProtocolConstants.DataType.INT:
                        genericRecord.put(field.toString(), udtValue.getInt(field));
                        break;
                    case ProtocolConstants.DataType.INET:
                        genericRecord.put(field.toString(), udtValue.getInetAddress(field).getHostAddress());
                        break;
                    case ProtocolConstants.DataType.BIGINT:
                        genericRecord.put(field.toString(), udtValue.getLong(field));
                        break;
                    case ProtocolConstants.DataType.DOUBLE:
                        genericRecord.put(field.toString(), udtValue.getDouble(field));
                        break;
                    case ProtocolConstants.DataType.FLOAT:
                        genericRecord.put(field.toString(), udtValue.getFloat(field));
                        break;
                    case ProtocolConstants.DataType.BOOLEAN:
                        genericRecord.put(field.toString(), udtValue.getBoolean(field));
                        break;
                    case ProtocolConstants.DataType.TIMESTAMP:
                        genericRecord.put(field.toString(), udtValue.getInstant(field).toEpochMilli());
                        break;
                    case ProtocolConstants.DataType.DATE:
                        genericRecord.put(field.toString(), (int) udtValue.getLocalDate(field).toEpochDay());
                        break;
                    case ProtocolConstants.DataType.TIME:
                        genericRecord.put(field.toString(), (udtValue.getLocalTime(field).toNanoOfDay() / 1000));
                        break;
                    case ProtocolConstants.DataType.BLOB:
                        genericRecord.put(field.toString(), udtValue.getByteBuffer(field));
                        break;
                    case ProtocolConstants.DataType.UDT:
                        genericRecord.put(field.toString(), buildUDTValue(udtValue.getUdtValue(field)));
                        break;
                    case ProtocolConstants.DataType.DURATION:
                        genericRecord.put(field.toString(), udtValue.getCqlDuration(field));
                        break;
                    case ProtocolConstants.DataType.DECIMAL:
                        genericRecord.put(field.toString(), udtValue.getBigDecimal(field));
                        break;
                    case ProtocolConstants.DataType.VARINT:
                        genericRecord.put(field.toString(), udtValue.getBigInteger(field));
                        break;
                    default:
                        log.debug("Ignoring unsupported type field name={} type={}", field, dataType.asCql(false, true));
                }
            }
        }
        return genericRecord;
    }

    public boolean isSupportedCqlType(DataType dataType) {
        switch (dataType.getProtocolCode()) {
            case ProtocolConstants.DataType.ASCII:
            case ProtocolConstants.DataType.VARCHAR:
            case ProtocolConstants.DataType.BOOLEAN:
            case ProtocolConstants.DataType.BLOB:
            case ProtocolConstants.DataType.DATE:
            case ProtocolConstants.DataType.TIME:
            case ProtocolConstants.DataType.TIMESTAMP:
            case ProtocolConstants.DataType.UUID:
            case ProtocolConstants.DataType.TIMEUUID:
            case ProtocolConstants.DataType.TINYINT:
            case ProtocolConstants.DataType.SMALLINT:
            case ProtocolConstants.DataType.INT:
            case ProtocolConstants.DataType.BIGINT:
            case ProtocolConstants.DataType.DOUBLE:
            case ProtocolConstants.DataType.FLOAT:
            case ProtocolConstants.DataType.INET:
            case ProtocolConstants.DataType.UDT:
            case ProtocolConstants.DataType.VARINT:
            case ProtocolConstants.DataType.DECIMAL:
            case ProtocolConstants.DataType.DURATION:
                return true;
        }
        return false;
    }

    /**
     * Convert GenericRecord to primary key column values.
     *
     * @param genericRecord
     * @return list of primary key column values
     */
    @Override
    public List<Object> fromConnectData(GenericRecord genericRecord) {
        List<Object> pk = new ArrayList<>(tableMetadata.getPrimaryKey().size());
        for(ColumnMetadata cm : tableMetadata.getPrimaryKey()) {
            Object value = genericRecord.get(cm.getName().asInternal());
            if (value != null) {
                switch (cm.getType().getProtocolCode()) {
                    case ProtocolConstants.DataType.ASCII:
                    case ProtocolConstants.DataType.VARCHAR:
                        value = value.toString();
                        break;
                    case ProtocolConstants.DataType.INET:
                        value = InetAddresses.forString(value.toString());
                        break;
                    case ProtocolConstants.DataType.UUID:
                    case ProtocolConstants.DataType.TIMEUUID:
                        value = UUID.fromString(value.toString());
                        break;
                    case ProtocolConstants.DataType.TINYINT:
                        value = ((Integer) value).byteValue();
                        break;
                    case ProtocolConstants.DataType.SMALLINT:
                        value = ((Integer) value).shortValue();
                        break;
                    case ProtocolConstants.DataType.TIMESTAMP:
                        value = Instant.ofEpochMilli((long) value);
                        break;
                    case ProtocolConstants.DataType.DATE:
                        value = LocalDate.ofEpochDay((int) value);
                        break;
                    case ProtocolConstants.DataType.TIME:
                        value = LocalTime.ofNanoOfDay( (long)value * 1000);
                        break;
                    case ProtocolConstants.DataType.DURATION: {
                        Conversion<CqlDuration> conversion = SpecificData.get().getConversionByClass(CqlDuration.class);
                        value = conversion.fromRecord( (IndexedRecord) value, CqlLogicalTypes.durationType, CqlLogicalTypes.CQL_DURATION_LOGICAL_TYPE);
                    }
                    break;
                    case ProtocolConstants.DataType.DECIMAL: {
                        Conversion<BigDecimal> conversion = SpecificData.get().getConversionByClass(BigDecimal.class);
                        value =  conversion.fromRecord( (IndexedRecord) value, CqlLogicalTypes.decimalType, CqlLogicalTypes.CQL_DECIMAL_LOGICAL_TYPE);
                    }
                    break;
                    case ProtocolConstants.DataType.VARINT: {
                        Conversion<BigInteger> conversion = SpecificData.get().getConversionByClass(BigInteger.class);
                        value = conversion.fromBytes((ByteBuffer) value, CqlLogicalTypes.varintType, CqlLogicalTypes.CQL_VARINT_LOGICAL_TYPE);
                    }
                    break;
                }
            }
            pk.add(value);
        }
        return pk;
    }

    public static class CqlDurationConversion extends Conversion<CqlDuration> {
        @Override
        public Class<CqlDuration> getConvertedType() {
            return CqlDuration.class;
        }

        @Override
        public String getLogicalTypeName() {
            return CqlLogicalTypes.CQL_DURATION;
        }

        @Override
        public CqlDuration fromRecord(IndexedRecord value, Schema schema, LogicalType type) {
            return CqlDuration.newInstance((int)value.get(0), (int)value.get(1), (long)value.get(2));
        }

        @Override
        public IndexedRecord toRecord(CqlDuration value, Schema schema, LogicalType type) {
            GenericRecord record = new GenericData.Record(CqlLogicalTypes.durationType);
            record.put(CqlLogicalTypes.CQL_DURATION_MONTHS, value.getMonths());
            record.put(CqlLogicalTypes.CQL_DURATION_DAYS, value.getDays());
            record.put(CqlLogicalTypes.CQL_DURATION_NANOSECONDS, value.getNanoseconds());
            return record;
        }
    }
}
