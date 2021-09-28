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

import com.datastax.cassandra.cdc.CqlLogicalTypes;
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
import com.google.common.net.InetAddresses;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.Schema.*;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;


import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;


@Slf4j
public class NativeAvroConverter implements Converter<org.apache.pulsar.client.api.Schema<byte[]>, GenericRecord, Row, List<Object>> {

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
            fields.add(fieldSchema(ksm, cm.getName().toString(), cm.getType(), !isPartitionKey));
        }
        this.avroSchema = Schema.createRecord(keyspaceAndTable, "Table " + keyspaceAndTable, ksm.getName().asInternal(), false, fields);
        this.pulsarSchema = org.apache.pulsar.client.api.Schema.NATIVE_AVRO(avroSchema);
        if (log.isInfoEnabled()) {
            log.info("schema={}", schemaToString(this.avroSchema));
            for(Map.Entry<String, Schema> entry : udtSchemas.entrySet()) {
                log.info("type={} schema={}", entry.getKey(), schemaToString(entry.getValue()));
            }
        }
    }

    public static String schemaToString(Schema schema) {
        return schema.toString();
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
            default:
                log.debug("Ignoring unsupported type fields name={} type={}", fieldName, dataType.asCql(false, true));
        }

        if (optional) {
            fieldSchema = new Schema.Field(fieldName, SchemaBuilder.unionOf().nullType().and().type(fieldSchema.schema()).endUnion());
        }
        return fieldSchema;
    }

    Schema buildUDTSchema(KeyspaceMetadata ksm, String typeName, boolean optional) {
        UserDefinedType userDefinedType = ksm.getUserDefinedType(CqlIdentifier.fromCql(typeName.substring(typeName.indexOf(".") + 1)))
                .orElseThrow(() -> new IllegalStateException("UDT " + typeName + " not found"));
        List<Schema.Field> fieldSchemas = new ArrayList<>();
        int i = 0;
        for(CqlIdentifier field : userDefinedType.getFieldNames()) {
            fieldSchemas.add(fieldSchema(ksm, field.toString(), userDefinedType.getFieldTypes().get(i++), optional));
        }
        return Schema.createRecord(ksm.getName() + "." + typeName, "", ksm.getName().asInternal(), false, fieldSchemas);
    }

    @Override
    public org.apache.pulsar.client.api.Schema getSchema() {
        return this.pulsarSchema;
    }

    @Override
    public GenericRecord toConnectData(Row row) {
        GenericRecord genericRecordBuilder = new GenericData.Record(avroSchema);
        log.info("row columns={}", row.getColumnDefinitions());
        for(ColumnDefinition cm : row.getColumnDefinitions()) {
            if (!row.isNull(cm.getName())) {
                switch (cm.getType().getProtocolCode()) {
                    case ProtocolConstants.DataType.UUID:
                    case ProtocolConstants.DataType.TIMEUUID:
                        genericRecordBuilder.put(cm.getName().toString(), row.getUuid(cm.getName()).toString());
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
                        genericRecordBuilder.put(cm.getName().toString(), (int) (row.getLocalTime(cm.getName()).toNanoOfDay() / 1000000));
                        break;
                    case ProtocolConstants.DataType.BLOB:
                        genericRecordBuilder.put(cm.getName().toString(), row.getByteBuffer(cm.getName()));
                        break;
                    case ProtocolConstants.DataType.UDT:
                        genericRecordBuilder.put(cm.getName().toString(), buildUDTValue(row.getUdtValue(cm.getName())));
                        break;
                    case ProtocolConstants.DataType.DURATION:
                        genericRecordBuilder.put(cm.getName().toString(), buildCqlDuration(row.getCqlDuration(cm.getName())));
                        break;
                    case ProtocolConstants.DataType.DECIMAL:
                        genericRecordBuilder.put(cm.getName().toString(), buildCqlDecimal(row.getBigDecimal(cm.getName())));
                        break;
                    case ProtocolConstants.DataType.VARINT:
                        genericRecordBuilder.put(cm.getName().toString(), row.getBigInteger(cm.getName()).toByteArray());
                        break;
                    default:
                        log.debug("Ignoring unsupported column name={} type={}", cm.getName(), cm.getType().asCql(false, true));
                }
            }
        }
        return genericRecordBuilder;
    }

    org.apache.avro.generic.GenericRecord buildCqlDuration(CqlDuration cqlDuration) {
        return new org.apache.avro.generic.GenericRecordBuilder(CqlLogicalTypes.durationType)
                .set(CqlLogicalTypes.CQL_DURATION_MONTHS, cqlDuration.getMonths())
                .set(CqlLogicalTypes.CQL_DURATION_DAYS, cqlDuration.getDays())
                .set(CqlLogicalTypes.CQL_DURATION_NANOSECONDS, cqlDuration.getNanoseconds())
                .build();
    }

    org.apache.avro.generic.GenericRecord buildCqlDecimal(BigDecimal bigDecimal) {
        return new org.apache.avro.generic.GenericRecordBuilder(CqlLogicalTypes.decimalType)
                .set(CqlLogicalTypes.CQL_DECIMAL_BIGINT, bigDecimal.unscaledValue())
                .set(CqlLogicalTypes.CQL_DECIMAL_SCALE, bigDecimal.scale())
                .build();
    }

    GenericRecord buildUDTValue(UdtValue udtValue) {
        String typeName = udtValue.getType().getKeyspace() + "." + udtValue.getType().getName().toString();
        Schema genericSchema = udtSchemas.get(typeName);
        assert genericSchema != null : "Generic schema not found for UDT=" + typeName;
        List<String> fields = genericSchema.getFields().stream().map(Field::name).collect(Collectors.toList());
        GenericRecord genericRecordBuilder = new GenericData.Record(genericSchema);
        for(CqlIdentifier field : udtValue.getType().getFieldNames()) {
            if (fields.contains(field.asInternal()) && !udtValue.isNull(field)) {
                DataType dataType = udtValue.getType(field);
                switch (dataType.getProtocolCode()) {
                    case ProtocolConstants.DataType.UUID:
                    case ProtocolConstants.DataType.TIMEUUID:
                        genericRecordBuilder.put(field.toString(), udtValue.getUuid(field).toString());
                        break;
                    case ProtocolConstants.DataType.ASCII:
                    case ProtocolConstants.DataType.VARCHAR:
                        genericRecordBuilder.put(field.toString(), udtValue.getString(field));
                        break;
                    case ProtocolConstants.DataType.TINYINT:
                        genericRecordBuilder.put(field.toString(), (int) udtValue.getByte(field));
                        break;
                    case ProtocolConstants.DataType.SMALLINT:
                        genericRecordBuilder.put(field.toString(), (int) udtValue.getShort(field));
                        break;
                    case ProtocolConstants.DataType.INT:
                        genericRecordBuilder.put(field.toString(), udtValue.getInt(field));
                        break;
                    case ProtocolConstants.DataType.INET:
                        genericRecordBuilder.put(field.toString(), udtValue.getInetAddress(field).getHostAddress());
                        break;
                    case ProtocolConstants.DataType.BIGINT:
                        genericRecordBuilder.put(field.toString(), udtValue.getLong(field));
                        break;
                    case ProtocolConstants.DataType.DOUBLE:
                        genericRecordBuilder.put(field.toString(), udtValue.getDouble(field));
                        break;
                    case ProtocolConstants.DataType.FLOAT:
                        genericRecordBuilder.put(field.toString(), udtValue.getFloat(field));
                        break;
                    case ProtocolConstants.DataType.BOOLEAN:
                        genericRecordBuilder.put(field.toString(), udtValue.getBoolean(field));
                        break;
                    case ProtocolConstants.DataType.TIMESTAMP:
                        genericRecordBuilder.put(field.toString(), udtValue.getInstant(field).toEpochMilli());
                        break;
                    case ProtocolConstants.DataType.DATE:
                        genericRecordBuilder.put(field.toString(), (int) udtValue.getLocalDate(field).toEpochDay());
                        break;
                    case ProtocolConstants.DataType.TIME:
                        genericRecordBuilder.put(field.toString(), (int) (udtValue.getLocalTime(field).toNanoOfDay() / 1000000));
                        break;
                    case ProtocolConstants.DataType.BLOB:
                        genericRecordBuilder.put(field.toString(), udtValue.getByteBuffer(field));
                        break;
                    case ProtocolConstants.DataType.UDT:
                        genericRecordBuilder.put(field.toString(), buildUDTValue(udtValue.getUdtValue(field)));
                        break;
                    case ProtocolConstants.DataType.DURATION:
                        genericRecordBuilder.put(field.toString(), buildCqlDuration(udtValue.getCqlDuration(field)));
                        break;
                    case ProtocolConstants.DataType.DECIMAL:
                        genericRecordBuilder.put(field.toString(), buildCqlDecimal(udtValue.getBigDecimal(field)));
                        break;
                    case ProtocolConstants.DataType.VARINT:
                        genericRecordBuilder.put(field.toString(), udtValue.getBigInteger(field).toByteArray());
                        break;
                    default:
                        log.debug("Ignoring unsupported type field name={} type={}", field, dataType.asCql(false, true));
                }
            }
        }
        return genericRecordBuilder;
    }

    @Override
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
                    case ProtocolConstants.DataType.INET:
                        value = InetAddresses.forString((String)value);
                        break;
                    case ProtocolConstants.DataType.UUID:
                    case ProtocolConstants.DataType.TIMEUUID:
                        value = UUID.fromString((String) value);
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
                        value = LocalTime.ofNanoOfDay( ((Integer)value).longValue() * 1000000);
                        break;
                    case ProtocolConstants.DataType.DURATION: {
                        org.apache.avro.generic.GenericRecord genericRecord1 = (org.apache.avro.generic.GenericRecord) value;
                        value = CqlDuration.newInstance(
                                (int) genericRecord1.get(CqlLogicalTypes.CQL_DURATION_MONTHS),
                                (int) genericRecord1.get(CqlLogicalTypes.CQL_DURATION_DAYS),
                                (long) genericRecord1.get(CqlLogicalTypes.CQL_DURATION_NANOSECONDS)
                        );
                    }
                    break;
                    case ProtocolConstants.DataType.DECIMAL: {
                        org.apache.avro.generic.GenericRecord genericRecord1 = (org.apache.avro.generic.GenericRecord) value;
                        BigInteger bigint = (BigInteger) genericRecord1.get(CqlLogicalTypes.CQL_DECIMAL_BIGINT);
                        int scale = (int) genericRecord1.get(CqlLogicalTypes.CQL_DECIMAL_SCALE);
                        value = new BigDecimal(bigint, scale);
                    }
                    break;
                    case ProtocolConstants.DataType.VARINT:
                        value = new BigInteger((byte[])value);
                    break;
                }
            }
            pk.add(value);
        }
        return pk;
    }
}
