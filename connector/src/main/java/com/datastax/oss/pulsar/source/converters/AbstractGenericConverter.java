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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.pulsar.source.Converter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.util.internal.JacksonUtils;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.*;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


@Slf4j
public abstract class AbstractGenericConverter implements Converter<GenericRecord, GenericRecord, Row, GenericRecord> {

    public final GenericSchema<GenericRecord> schema;
    public final SchemaInfo schemaInfo;
    public final SchemaType schemaType;
    public final TableMetadata tableMetadata;
    public final Map<String, GenericSchema<GenericRecord>> udtSchemas = new HashMap<>();

    public AbstractGenericConverter(KeyspaceMetadata ksm, TableMetadata tm, List<ColumnMetadata> columns, SchemaType schemaType) {
        this.tableMetadata = tm;
        RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record(ksm.getName() + "." + tm.getName());
        for(ColumnMetadata cm : columns) {
            boolean isPartitionKey = tm.getPartitionKey().contains(cm);
            if (isSupportedCqlType(cm.getType())) {
                addFieldSchema(recordSchemaBuilder, ksm, cm.getName().toString(), cm.getType(), schemaType, !isPartitionKey);
            }
        }
        this.schemaInfo = recordSchemaBuilder.build(schemaType);
        this.schema = Schema.generic(schemaInfo);
        this.schemaType = schemaType;
        if (log.isInfoEnabled()) {
            log.info("schema={}", this.schema);
            for(Map.Entry<String, GenericSchema<GenericRecord>> entry : udtSchemas.entrySet()) {
                log.info("type={} schema={}", entry.getKey(), entry.getValue());
            }
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
                return true;
        }
        return false;
    }

    RecordSchemaBuilder addFieldSchema(RecordSchemaBuilder recordSchemaBuilder,
                                       KeyspaceMetadata ksm,
                                       String fieldName,
                                       DataType dataType,
                                       SchemaType schemaType,
                                       boolean optional) {
        switch(dataType.getProtocolCode()) {
            case ProtocolConstants.DataType.INET:
            case ProtocolConstants.DataType.UUID:
            case ProtocolConstants.DataType.TIMEUUID:
            case ProtocolConstants.DataType.ASCII:
            case ProtocolConstants.DataType.VARCHAR: {
                FieldSchemaBuilder fieldSchemaBuilder = recordSchemaBuilder.field(fieldName).type(SchemaType.STRING);
                if (optional)
                    fieldSchemaBuilder.optional().defaultValue(null);
            }
            break;
            case ProtocolConstants.DataType.BLOB: {
                FieldSchemaBuilder fieldSchemaBuilder = recordSchemaBuilder.field(fieldName).type(SchemaType.BYTES);
                if (optional)
                    fieldSchemaBuilder.optional().defaultValue(null);
            }
            break;
            case ProtocolConstants.DataType.TINYINT:
            case ProtocolConstants.DataType.SMALLINT:
            case ProtocolConstants.DataType.INT: {
                FieldSchemaBuilder fieldSchemaBuilder = recordSchemaBuilder.field(fieldName).type(SchemaType.INT32);
                if (optional)
                    fieldSchemaBuilder.optional().defaultValue(null);
            }
            break;
            case ProtocolConstants.DataType.BIGINT: {
                FieldSchemaBuilder fieldSchemaBuilder = recordSchemaBuilder.field(fieldName).type(SchemaType.INT64);
                if (optional)
                    fieldSchemaBuilder.optional().defaultValue(null);
            }
            break;
            case ProtocolConstants.DataType.BOOLEAN: {
                FieldSchemaBuilder fieldSchemaBuilder = recordSchemaBuilder.field(fieldName).type(SchemaType.BOOLEAN);
                if (optional)
                    fieldSchemaBuilder.optional().defaultValue(null);
            }
            break;
            case ProtocolConstants.DataType.FLOAT: {
                FieldSchemaBuilder fieldSchemaBuilder = recordSchemaBuilder.field(fieldName).type(SchemaType.FLOAT);
                if (optional)
                    fieldSchemaBuilder.optional().defaultValue(null);
            }
            break;
            case ProtocolConstants.DataType.DOUBLE: {
                FieldSchemaBuilder fieldSchemaBuilder = recordSchemaBuilder.field(fieldName).type(SchemaType.DOUBLE);
                if (optional)
                    fieldSchemaBuilder.optional().defaultValue(null);
            }
            break;
            case ProtocolConstants.DataType.TIMESTAMP: {
                FieldSchemaBuilder fieldSchemaBuilder = recordSchemaBuilder.field(fieldName).type(SchemaType.TIMESTAMP);
                if (optional)
                    fieldSchemaBuilder.optional().defaultValue(null);
            }
            break;
            case ProtocolConstants.DataType.DATE: {
                FieldSchemaBuilder fieldSchemaBuilder = recordSchemaBuilder.field(fieldName).type(SchemaType.DATE);
                if (optional)
                    fieldSchemaBuilder.optional().defaultValue(null);
            }
            break;
            case ProtocolConstants.DataType.TIME: {
                FieldSchemaBuilder fieldSchemaBuilder = recordSchemaBuilder.field(fieldName).type(SchemaType.TIME);
                if (optional)
                    fieldSchemaBuilder.optional().defaultValue(null);
            }
            break;
            case ProtocolConstants.DataType.UDT: {
                FieldSchemaBuilder fieldSchemaBuilder = recordSchemaBuilder
                        .field(fieldName, buildUDTSchema(ksm, dataType.asCql(false, true), schemaType, optional))
                        .type(schemaType);
                if (optional)
                    fieldSchemaBuilder.optional().defaultValue(null);
            }
            break;
            default:
                log.debug("Ignoring unsupported type fields name={} type={}", fieldName, dataType.asCql(false, true));
        }
        return recordSchemaBuilder;
    }

    GenericSchema<GenericRecord> buildUDTSchema(KeyspaceMetadata ksm, String typeName, SchemaType schemaType, boolean optional) {
        UserDefinedType userDefinedType = ksm.getUserDefinedType(CqlIdentifier.fromCql(typeName.substring(typeName.indexOf(".") + 1)))
                .orElseThrow(() -> new IllegalStateException("UDT " + typeName + " not found"));
        RecordSchemaBuilder udtSchemaBuilder = SchemaBuilder.record(typeName);
        int i = 0;
        for(CqlIdentifier field : userDefinedType.getFieldNames()) {
            addFieldSchema(udtSchemaBuilder, ksm, field.toString(), userDefinedType.getFieldTypes().get(i++), schemaType, optional);
        }
        SchemaInfo pcGenericSchemaInfo = udtSchemaBuilder.build(schemaType);
        GenericSchema<GenericRecord> genericSchema = Schema.generic(pcGenericSchemaInfo);
        udtSchemas.put(typeName, genericSchema);
        return genericSchema;
    }

    @Override
    public Schema<GenericRecord> getSchema() {
        return this.schema;
    }

    @Override
    public GenericRecord toConnectData(Row row) {
        GenericRecordBuilder genericRecordBuilder = schema.newRecordBuilder();
        for(ColumnDefinition cm : row.getColumnDefinitions()) {
            if (!row.isNull(cm.getName())) {
                switch (cm.getType().getProtocolCode()) {
                    case ProtocolConstants.DataType.UUID:
                    case ProtocolConstants.DataType.TIMEUUID:
                        genericRecordBuilder.set(cm.getName().toString(), row.getUuid(cm.getName()).toString());
                        break;
                    case ProtocolConstants.DataType.ASCII:
                    case ProtocolConstants.DataType.VARCHAR:
                        genericRecordBuilder.set(cm.getName().toString(), row.getString(cm.getName()));
                        break;
                    case ProtocolConstants.DataType.TINYINT:
                        genericRecordBuilder.set(cm.getName().toString(), (int) row.getByte(cm.getName()));
                        break;
                    case ProtocolConstants.DataType.SMALLINT:
                        genericRecordBuilder.set(cm.getName().toString(), (int) row.getShort(cm.getName()));
                        break;
                    case ProtocolConstants.DataType.INT:
                        genericRecordBuilder.set(cm.getName().toString(), row.getInt(cm.getName()));
                        break;
                    case ProtocolConstants.DataType.BIGINT:
                        genericRecordBuilder.set(cm.getName().toString(), row.getLong(cm.getName()));
                        break;
                    case ProtocolConstants.DataType.INET:
                        genericRecordBuilder.set(cm.getName().toString(), row.getInetAddress(cm.getName()).getHostAddress());
                        break;
                    case ProtocolConstants.DataType.DOUBLE:
                        genericRecordBuilder.set(cm.getName().toString(), row.getDouble(cm.getName()));
                        break;
                    case ProtocolConstants.DataType.FLOAT:
                        genericRecordBuilder.set(cm.getName().toString(), row.getFloat(cm.getName()));
                        break;
                    case ProtocolConstants.DataType.BOOLEAN:
                        genericRecordBuilder.set(cm.getName().toString(), row.getBoolean(cm.getName()));
                        break;
                    case ProtocolConstants.DataType.TIMESTAMP:
                        genericRecordBuilder.set(cm.getName().toString(), row.getInstant(cm.getName()).toEpochMilli());
                        break;
                    case ProtocolConstants.DataType.DATE: // Avro date is epoch days
                        genericRecordBuilder.set(cm.getName().toString(), (int) row.getLocalDate(cm.getName()).toEpochDay());
                        break;
                    case ProtocolConstants.DataType.TIME: // Avro time is epoch milliseconds
                        genericRecordBuilder.set(cm.getName().toString(), (int) (row.getLocalTime(cm.getName()).toNanoOfDay() / 1000000));
                        break;
                    case ProtocolConstants.DataType.BLOB:
                        genericRecordBuilder.set(cm.getName().toString(), row.getByteBuffer(cm.getName()));
                        break;
                    case ProtocolConstants.DataType.UDT:
                        genericRecordBuilder.set(cm.getName().toString(), buildUDTValue(row.getUdtValue(cm.getName())));
                        break;
                    default:
                        log.debug("Ignoring unsupported column name={} type={}", cm.getName(), cm.getType().asCql(false, true));
                }
            }
        }
        return genericRecordBuilder.build();
    }

    GenericRecord buildUDTValue(UdtValue udtValue) {
        String typeName = udtValue.getType().getKeyspace() + "." + udtValue.getType().getName().toString();
        GenericSchema<?> genericSchema = udtSchemas.get(typeName);
        assert genericSchema != null : "Generic schema not found for UDT=" + typeName;
        List<String> fields = genericSchema.getFields().stream().map(Field::getName).collect(Collectors.toList());
        GenericRecordBuilder genericRecordBuilder = genericSchema.newRecordBuilder();
        for(CqlIdentifier field : udtValue.getType().getFieldNames()) {
            if (fields.contains(field.asInternal()) && !udtValue.isNull(field)) {
                DataType dataType = udtValue.getType(field);
                switch (dataType.getProtocolCode()) {
                    case ProtocolConstants.DataType.UUID:
                    case ProtocolConstants.DataType.TIMEUUID:
                        genericRecordBuilder.set(field.toString(), udtValue.getUuid(field).toString());
                        break;
                    case ProtocolConstants.DataType.ASCII:
                    case ProtocolConstants.DataType.VARCHAR:
                        genericRecordBuilder.set(field.toString(), udtValue.getString(field));
                        break;
                    case ProtocolConstants.DataType.TINYINT:
                        genericRecordBuilder.set(field.toString(), (int) udtValue.getByte(field));
                        break;
                    case ProtocolConstants.DataType.SMALLINT:
                        genericRecordBuilder.set(field.toString(), (int) udtValue.getShort(field));
                        break;
                    case ProtocolConstants.DataType.INT:
                        genericRecordBuilder.set(field.toString(), udtValue.getInt(field));
                        break;
                    case ProtocolConstants.DataType.INET:
                        genericRecordBuilder.set(field.toString(), udtValue.getInetAddress(field).getHostAddress());
                        break;
                    case ProtocolConstants.DataType.BIGINT:
                        genericRecordBuilder.set(field.toString(), udtValue.getLong(field));
                        break;
                    case ProtocolConstants.DataType.DOUBLE:
                        genericRecordBuilder.set(field.toString(), udtValue.getDouble(field));
                        break;
                    case ProtocolConstants.DataType.FLOAT:
                        genericRecordBuilder.set(field.toString(), udtValue.getFloat(field));
                        break;
                    case ProtocolConstants.DataType.BOOLEAN:
                        genericRecordBuilder.set(field.toString(), udtValue.getBoolean(field));
                        break;
                    case ProtocolConstants.DataType.TIMESTAMP:
                        genericRecordBuilder.set(field.toString(), udtValue.getInstant(field).toEpochMilli());
                        break;
                    case ProtocolConstants.DataType.DATE:
                        genericRecordBuilder.set(field.toString(), (int) udtValue.getLocalDate(field).toEpochDay());
                        break;
                    case ProtocolConstants.DataType.TIME:
                        genericRecordBuilder.set(field.toString(), (int) (udtValue.getLocalTime(field).toNanoOfDay() / 1000000));
                        break;
                    case ProtocolConstants.DataType.BLOB:
                        genericRecordBuilder.set(field.toString(), udtValue.getByteBuffer(field));
                        break;
                    case ProtocolConstants.DataType.UDT:
                        genericRecordBuilder.set(field.toString(), buildUDTValue(udtValue.getUdtValue(field)));
                        break;
                    default:
                        log.debug("Ignoring unsupported type field name={} type={}", field, dataType.asCql(false, true));
                }
            }
        }
        return genericRecordBuilder.build();
    }

    /**
     * Convert GenericRecord from the dirty topic to a GenericRecord of the data topic that are sink ready.
     *
     * @param genericRecord the avro key generic record read from the dirty topic
     * @return schema type encoded key (e.g. JSON)
     */
    @Override
    public GenericRecord fromConnectData(GenericRecord genericRecord) {
        if (genericRecord.getSchemaType() == this.schemaType) {
            return genericRecord;
        }

        if (genericRecord.getSchemaType() == SchemaType.AVRO) {
            GenericRecordBuilder builder = this.schema.newRecordBuilder();
            for (Field field: genericRecord.getFields()) {
                // handle nested AVRO objects
                builder.set(field, JacksonUtils.toJsonNode(genericRecord.getField(field)));
            }
            return builder.build();
        }

        throw new UnsupportedOperationException(
                String.format("Cannot convert from %s to a generic record with schema type %s" ,
                genericRecord, this.schemaType));
    }
}
