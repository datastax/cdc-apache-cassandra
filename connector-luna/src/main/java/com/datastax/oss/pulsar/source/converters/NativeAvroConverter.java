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
import com.datastax.oss.driver.api.core.type.*;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.type.PrimitiveType;
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
import org.apache.avro.generic.GenericArray;
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
import java.net.InetAddress;
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
    public final Map<String, Schema> collectionSchemas = new HashMap<>();

    public NativeAvroConverter(KeyspaceMetadata ksm, TableMetadata tm, List<ColumnMetadata> columns) {
        this.tableMetadata = tm;
        String keyspaceAndTable = ksm.getName() + "." + tm.getName();
        List<Schema.Field> fields = new ArrayList<>();
        for(ColumnMetadata cm : columns) {
            boolean isPartitionKey = tm.getPartitionKey().contains(cm);
            if (isSupportedCqlType(cm.getType())) {
                Schema.Field field = fieldSchema(ksm, cm.getName().toString(), cm.getType(), !isPartitionKey);
                if (field != null) {
                    fields.add(field);
                    switch(cm.getType().getProtocolCode()) {
                        case ProtocolConstants.DataType.LIST:
                        case ProtocolConstants.DataType.SET:
                        case ProtocolConstants.DataType.MAP:
                            Schema collectionSchema = dataTypeSchema(ksm, cm.getType());
                            collectionSchemas.put(field.name(), collectionSchema);
                            log.info("Add collection schema {}={}", field.name(), collectionSchema);
                    }
                }
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
        return fieldSchema(ksm, fieldName, dataTypeSchema(ksm, dataType), optional);
    }

    Schema.Field fieldSchema(KeyspaceMetadata ksm,
                       String fieldName,
                       Schema schema,
                       boolean optional) {
        Schema.Field fieldSchema = new Schema.Field(fieldName, schema);
        if (optional) {
            fieldSchema = new Schema.Field(fieldName, SchemaBuilder.unionOf().nullType().and().type(fieldSchema.schema()).endUnion(), null, Field.NULL_DEFAULT_VALUE);
        }
        return fieldSchema;
    }

    Schema dataTypeSchema(KeyspaceMetadata ksm, DataType dataType) {
        switch(dataType.getProtocolCode()) {
            case ProtocolConstants.DataType.INET:
            case ProtocolConstants.DataType.ASCII:
            case ProtocolConstants.DataType.VARCHAR:
                return org.apache.avro.Schema.create(Type.STRING);
            case ProtocolConstants.DataType.BLOB:
                return org.apache.avro.Schema.create(Type.BYTES);
            case ProtocolConstants.DataType.TINYINT:
            case ProtocolConstants.DataType.SMALLINT:
            case ProtocolConstants.DataType.INT:
                return org.apache.avro.Schema.create(Type.INT);
            case ProtocolConstants.DataType.BIGINT:
                return org.apache.avro.Schema.create(Type.LONG);
            case ProtocolConstants.DataType.BOOLEAN:
                return org.apache.avro.Schema.create(Type.BOOLEAN);
            case ProtocolConstants.DataType.FLOAT:
                return org.apache.avro.Schema.create(Type.FLOAT);
            case ProtocolConstants.DataType.DOUBLE:
                return org.apache.avro.Schema.create(Type.DOUBLE);
            case ProtocolConstants.DataType.TIMESTAMP:
                return CqlLogicalTypes.timestampMillisType;
            case ProtocolConstants.DataType.DATE:
                return CqlLogicalTypes.dateType;
            case ProtocolConstants.DataType.TIME:
                return CqlLogicalTypes.timeMicrosType;
            case ProtocolConstants.DataType.UDT:
                return buildUDTSchema(ksm, dataType.asCql(false, true), false);
            case ProtocolConstants.DataType.UUID:
            case ProtocolConstants.DataType.TIMEUUID:
                return CqlLogicalTypes.uuidType;
            case ProtocolConstants.DataType.VARINT:
                return CqlLogicalTypes.varintType;
            case ProtocolConstants.DataType.DECIMAL:
                return CqlLogicalTypes.decimalType;
            case ProtocolConstants.DataType.DURATION:
                return CqlLogicalTypes.durationType;
            case ProtocolConstants.DataType.LIST:
                ListType listType = (ListType) dataType;
                return org.apache.avro.Schema.createArray(dataTypeSchema(ksm, listType.getElementType()));
            case ProtocolConstants.DataType.SET:
                SetType setType = (SetType) dataType;
                return org.apache.avro.Schema.createArray(dataTypeSchema(ksm, setType.getElementType()));
            case ProtocolConstants.DataType.MAP:
                MapType mapType = (MapType) dataType;
                return org.apache.avro.Schema.createMap(dataTypeSchema(ksm, mapType.getValueType()));
            default:
                throw new UnsupportedOperationException("Ignoring unsupported type=" + dataType.asCql(false, true));
        }
    }

    Schema buildUDTSchema(KeyspaceMetadata ksm, String typeName, boolean optional) {
        UserDefinedType userDefinedType = ksm.getUserDefinedType(CqlIdentifier.fromCql(typeName.substring(typeName.indexOf(".") + 1)))
                .orElseThrow(() -> new IllegalStateException("UDT " + typeName + " not found"));
        List<Schema.Field> fieldSchemas = new ArrayList<>();
        int i = 0;
        for(CqlIdentifier field : userDefinedType.getFieldNames()) {
            Schema.Field fieldSchema = fieldSchema(ksm, field.toString(), userDefinedType.getFieldTypes().get(i), optional);
            if (fieldSchema != null) {
                fieldSchemas.add(fieldSchema);
                String path = typeName + "." + field.toString();
                collectionSchemas.put(path, dataTypeSchema(ksm, userDefinedType.getFieldTypes().get(i)));
            }
            i++;
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
            String fieldName = cm.getName().toString();
            if (!row.isNull(cm.getName())) {
                switch (cm.getType().getProtocolCode()) {
                    case ProtocolConstants.DataType.UUID:
                    case ProtocolConstants.DataType.TIMEUUID:
                        genericRecordBuilder.put(fieldName, row.getUuid(cm.getName()));
                        break;
                    case ProtocolConstants.DataType.ASCII:
                    case ProtocolConstants.DataType.VARCHAR:
                        genericRecordBuilder.put(fieldName, row.getString(cm.getName()));
                        break;
                    case ProtocolConstants.DataType.TINYINT:
                        genericRecordBuilder.put(fieldName, (int) row.getByte(cm.getName()));
                        break;
                    case ProtocolConstants.DataType.SMALLINT:
                        genericRecordBuilder.put(fieldName, (int) row.getShort(cm.getName()));
                        break;
                    case ProtocolConstants.DataType.INT:
                        genericRecordBuilder.put(fieldName, row.getInt(cm.getName()));
                        break;
                    case ProtocolConstants.DataType.BIGINT:
                        genericRecordBuilder.put(fieldName, row.getLong(cm.getName()));
                        break;
                    case ProtocolConstants.DataType.INET:
                        genericRecordBuilder.put(fieldName, row.getInetAddress(cm.getName()).getHostAddress());
                        break;
                    case ProtocolConstants.DataType.DOUBLE:
                        genericRecordBuilder.put(fieldName, row.getDouble(cm.getName()));
                        break;
                    case ProtocolConstants.DataType.FLOAT:
                        genericRecordBuilder.put(fieldName, row.getFloat(cm.getName()));
                        break;
                    case ProtocolConstants.DataType.BOOLEAN:
                        genericRecordBuilder.put(fieldName, row.getBoolean(cm.getName()));
                        break;
                    case ProtocolConstants.DataType.TIMESTAMP:
                        genericRecordBuilder.put(fieldName, row.getInstant(cm.getName()).toEpochMilli());
                        break;
                    case ProtocolConstants.DataType.DATE: // Avro date is epoch days
                        genericRecordBuilder.put(fieldName, (int) row.getLocalDate(cm.getName()).toEpochDay());
                        break;
                    case ProtocolConstants.DataType.TIME: // Avro time is epoch milliseconds
                        genericRecordBuilder.put(fieldName, (row.getLocalTime(cm.getName()).toNanoOfDay() / 1000));
                        break;
                    case ProtocolConstants.DataType.BLOB:
                        genericRecordBuilder.put(fieldName, row.getByteBuffer(cm.getName()));
                        break;
                    case ProtocolConstants.DataType.UDT:
                        genericRecordBuilder.put(fieldName, buildUDTValue(row.getUdtValue(cm.getName())));
                        break;
                    case ProtocolConstants.DataType.DURATION:
                        genericRecordBuilder.put(fieldName, row.getCqlDuration(cm.getName()));
                        break;
                    case ProtocolConstants.DataType.DECIMAL:
                        genericRecordBuilder.put(fieldName, row.getBigDecimal(cm.getName()));
                        break;
                    case ProtocolConstants.DataType.VARINT:
                        genericRecordBuilder.put(fieldName, row.getBigInteger(cm.getName()));
                        break;
                    case ProtocolConstants.DataType.LIST: {
                        ListType listType = (ListType) cm.getType();
                        Schema listSchema = collectionSchemas.get(fieldName);
                        List listValue = row.getList(fieldName, CodecRegistry.DEFAULT.codecFor(listType.getElementType()).getJavaType().getRawType());
                        log.info("field={} listSchema={} listValue={}", fieldName, listSchema, listValue);
                        genericRecordBuilder.put(fieldName, buildArrayValue(listSchema, listValue));
                    }
                    break;
                    case ProtocolConstants.DataType.SET: {
                        SetType setType = (SetType) cm.getType();
                        Schema setSchema = collectionSchemas.get(fieldName);
                        Set setValue = row.getSet(fieldName, CodecRegistry.DEFAULT.codecFor(setType.getElementType()).getJavaType().getRawType());
                        log.info("field={} setSchema={} setValue={}", fieldName, setSchema, setValue);
                        genericRecordBuilder.put(fieldName, buildArrayValue(setSchema, setValue));
                    }
                    break;
                    case ProtocolConstants.DataType.MAP: {
                        MapType mapType = (MapType) cm.getType();
                        Schema mapSchema = collectionSchemas.get(fieldName);
                        Map<String, Object> mapValue = row.getMap(fieldName,
                                        CodecRegistry.DEFAULT.codecFor(mapType.getKeyType()).getJavaType().getRawType(),
                                        CodecRegistry.DEFAULT.codecFor(mapType.getValueType()).getJavaType().getRawType())
                                .entrySet().stream().collect(Collectors.toMap(e -> stringify(mapType.getKeyType(), e.getKey()), Map.Entry::getValue));
                        log.info("field={} mapSchema={} mapValue={}", fieldName, mapSchema, mapValue);
                        genericRecordBuilder.put(fieldName, mapValue);
                    }
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
                    case ProtocolConstants.DataType.LIST: {
                            ListType listType = (ListType) udtValue.getType(field);
                            String path = typeName + "." + field.toString();
                            Schema elementSchema = collectionSchemas.get(path);
                            genericRecord.put(field.toString(), buildArrayValue(elementSchema, udtValue.getList(field, listType.getElementType().getClass())));
                        }
                        break;
                    case ProtocolConstants.DataType.SET: {
                            SetType setType = (SetType) udtValue.getType(field);
                            String path = typeName + "." + field.toString();
                            Schema elementSchema = collectionSchemas.get(path);
                            genericRecord.put(field.toString(), buildArrayValue(elementSchema, udtValue.getList(field, setType.getElementType().getClass())));
                        }
                        break;
                    case ProtocolConstants.DataType.MAP: {
                            MapType mapType = (MapType) udtValue.getType(field);
                            String path = typeName + "." + field.toString();
                            Schema valueSchema = collectionSchemas.get(path);
                            Map<String, Object> map = udtValue.getMap(field, mapType.getKeyType().getClass(), mapType.getValueType().getClass())
                                    .entrySet().stream().collect(Collectors.toMap(e -> stringify(mapType.getKeyType(), e.getKey()), Map.Entry::getValue));
                            genericRecord.put(field.toString(), map);
                        }
                        break;
                    default:
                        log.debug("Ignoring unsupported type field name={} type={}", field, dataType.asCql(false, true));
                }
            }
        }
        return genericRecord;
    }

    @SuppressWarnings("unchecked")
    GenericArray buildArrayValue(Schema schema, Collection collection) {
        GenericArray genericArray = new GenericData.Array<>(collection.size(), schema);
        for(Object element : collection)
            genericArray.add(element);
        return genericArray;
    }

    String stringify(DataType dataType, Object value) {
        switch (dataType.getProtocolCode()) {
            case ProtocolConstants.DataType.ASCII:
            case ProtocolConstants.DataType.VARCHAR:
            case ProtocolConstants.DataType.BOOLEAN:
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
            case ProtocolConstants.DataType.VARINT:
            case ProtocolConstants.DataType.DECIMAL:
            case ProtocolConstants.DataType.DURATION:
            case ProtocolConstants.DataType.LIST:
            case ProtocolConstants.DataType.SET:
            case ProtocolConstants.DataType.MAP:
                return value.toString();
            case ProtocolConstants.DataType.INET:
                return ((InetAddress)value).getHostAddress();
            case ProtocolConstants.DataType.UDT:
                //TODO: convert UDT to a map
            default:
                throw new UnsupportedOperationException("Unsupported type="+dataType.getProtocolCode()+" as key in a map");
        }
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
            case ProtocolConstants.DataType.LIST:
            case ProtocolConstants.DataType.SET:
            case ProtocolConstants.DataType.MAP:
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
                    // CQL collection types are not supported in the PK
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
