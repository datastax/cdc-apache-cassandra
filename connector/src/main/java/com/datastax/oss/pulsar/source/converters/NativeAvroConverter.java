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

import com.datastax.oss.cdc.CqlLogicalTypes;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.CqlVectorType;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.google.common.base.Preconditions;
import com.google.common.net.InetAddresses;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.pulsar.common.schema.SchemaType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * AVRO Converter providing support for logical types.
 */
@Slf4j
public class NativeAvroConverter extends AbstractNativeConverter<List<Object>> {

    public NativeAvroConverter(KeyspaceMetadata ksm, TableMetadata tm, List<ColumnMetadata> columns) {
        super(ksm, tm, columns);
    }

    @Override
    SchemaType getSchemaType() {
        return SchemaType.AVRO;
    }

    @Override
    public byte[] toConnectData(Row row) {
        GenericRecord genericRecordBuilder = new GenericData.Record(nativeSchema);
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
                        Schema listSchema = subSchemas.get(fieldName);
                        List listValue = row.getList(fieldName, CodecRegistry.DEFAULT.codecFor(listType.getElementType()).getJavaType().getRawType());
                        log.debug("field={} listSchema={} listValue={}", fieldName, listSchema, listValue);
                        genericRecordBuilder.put(fieldName, buildArrayValue(listSchema, listValue));
                    }
                    break;
                    case ProtocolConstants.DataType.SET: {
                        SetType setType = (SetType) cm.getType();
                        Schema setSchema = subSchemas.get(fieldName);
                        Set setValue = row.getSet(fieldName, CodecRegistry.DEFAULT.codecFor(setType.getElementType()).getJavaType().getRawType());
                        log.debug("field={} setSchema={} setValue={}", fieldName, setSchema, setValue);
                        genericRecordBuilder.put(fieldName, buildArrayValue(setSchema, setValue));
                    }
                    break;
                    case ProtocolConstants.DataType.MAP: {
                        MapType mapType = (MapType) cm.getType();
                        Schema mapSchema = subSchemas.get(fieldName);
                        Map<String, Object> mapValue = row.getMap(fieldName,
                                        CodecRegistry.DEFAULT.codecFor(mapType.getKeyType()).getJavaType().getRawType(),
                                        CodecRegistry.DEFAULT.codecFor(mapType.getValueType()).getJavaType().getRawType())
                                .entrySet().stream().collect(Collectors.toMap(e -> stringify(mapType.getKeyType(), e.getKey()), Map.Entry::getValue));
                        log.debug("field={} mapSchema={} mapValue={}", fieldName, mapSchema, mapValue);
                        genericRecordBuilder.put(fieldName, mapValue);
                    }
                    break;
                    case ProtocolConstants.DataType.CUSTOM: {
                        if (cm.getType() instanceof CqlVectorType) {
                            Schema vectorSchema = subSchemas.get(fieldName);
                            CqlVector<?> vector = row.getCqlVector(fieldName);
                            log.debug("field={} listSchema={} listValue={}", fieldName, vectorSchema, vector);
                            List<Object> vectorValue = new ArrayList<>();
                            vector.getValues().forEach(vectorValue::add);
                            genericRecordBuilder.put(fieldName, buildArrayValue(vectorSchema, vectorValue));
                        }
                    }
                    break;
                    default:
                        log.debug("Ignoring unsupported column name={} type={}", cm.getName(), cm.getType().asCql(false, true));
                }
            }
        }
        return serializeAvroGenericRecord(genericRecordBuilder, nativeSchema);
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
        Schema udtSchema = subSchemas.get(typeName);
        Preconditions.checkNotNull(udtSchema, "Schema not found for UDT=" + typeName);
        Preconditions.checkState(udtSchema.getFields().size() > 0, "Schema UDT=" + typeName + " has no fields");
        return buildUDTValue(udtSchema, udtValue);
    }

    GenericRecord buildUDTValue(Schema udtSchema, UdtValue udtValue) {
        String typeName = udtValue.getType().getKeyspace() + "." + udtValue.getType().getName();
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
                            List listValue = udtValue.getList(field, CodecRegistry.DEFAULT.codecFor(listType.getElementType()).getJavaType().getRawType());
                            String path = typeName + "." + field.toString();
                            Schema elementSchema = subSchemas.get(path);
                            log.debug("path={} elementSchema={} listType={} listValue={}", path, elementSchema, listType, listValue);
                            genericRecord.put(field.toString(), buildArrayValue(elementSchema, listValue));
                        }
                        break;
                    case ProtocolConstants.DataType.SET: {
                            SetType setType = (SetType) udtValue.getType(field);
                            Set setValue = udtValue.getSet(field, CodecRegistry.DEFAULT.codecFor(setType.getElementType()).getJavaType().getRawType());
                            String path = typeName + "." + field.toString();
                            Schema elementSchema = subSchemas.get(path);
                            log.debug("path={} elementSchema={} setType={} setValue={}", path, elementSchema, setType, setValue);
                            genericRecord.put(field.toString(), buildArrayValue(elementSchema, setValue));
                        }
                        break;
                    case ProtocolConstants.DataType.MAP: {
                            MapType mapType = (MapType) udtValue.getType(field);
                            Map<String, Object> mapValue = udtValue.getMap(field,
                                            CodecRegistry.DEFAULT.codecFor(mapType.getKeyType()).getJavaType().getRawType(),
                                            CodecRegistry.DEFAULT.codecFor(mapType.getValueType()).getJavaType().getRawType())
                                    .entrySet().stream().collect(Collectors.toMap(e -> stringify(mapType.getKeyType(), e.getKey()), Map.Entry::getValue));
                            String path = typeName + "." + field.toString();
                            Schema valueSchema = subSchemas.get(path);
                            log.debug("path={} valueSchema={} mapType={} mapValue={}", path, valueSchema, mapType, mapValue);
                            genericRecord.put(field.toString(), mapValue);
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
        for(Object element : collection) {
            if (element instanceof UdtValue) {
                UdtValue udtValue = (UdtValue) element;
                genericArray.add(buildUDTValue(udtValue));
            } else if (element instanceof Set) {
                genericArray.add(buildArrayValue(schema.getElementType(), (Collection)element));
            } else if (element instanceof List) {
                genericArray.add(buildArrayValue(schema.getElementType(), (Collection)element));
            } else {
                genericArray.add(element);
            }
        }
        return genericArray;
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
