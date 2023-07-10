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
import com.datastax.oss.cdc.NativeSchemaWrapper;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.CqlVectorType;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.pulsar.source.Converter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.common.schema.SchemaType;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An abstract class for native schema converters. Pulsar schemas (Avro and JSON) are represented in Avro with the only
 * difference in the type field.
 * @param <T> The desired type of the key representation. For example the Avro converter, the desired type is a List of
 *           PK columns to because to query C* table. However, the actual keys are copied as is from the mutation topic.
 *           In JSON only format, because the key will be embedded in the payload, the subclass may wish to convert to
 *           pulsar's GenericRecord or serialized Jackson node
 */
@Slf4j
public abstract class AbstractNativeConverter<T> implements Converter<byte[], GenericRecord, Row, T> {
    public final org.apache.pulsar.client.api.Schema<byte[]> pulsarSchema;
    public final Schema nativeSchema;
    public final TableMetadata tableMetadata;
    public final Map<String, Schema> subSchemas = new HashMap<>();

    public AbstractNativeConverter(KeyspaceMetadata ksm, TableMetadata tm, List<ColumnMetadata> columns) {
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
                            subSchemas.put(field.name(), collectionSchema);
                            log.info("Add collection schema {}={}", field.name(), collectionSchema);
                            break;
                        case ProtocolConstants.DataType.CUSTOM:
                            if (cm.getType() instanceof CqlVectorType) {
                                Schema vectorSchema = dataTypeSchema(ksm, cm.getType());
                                subSchemas.put(field.name(), vectorSchema);
                                log.info("Add vector schema {}={}", field.name(), vectorSchema);
                            }
                            break;
                    }
                }
            }
        }
        this.nativeSchema = Schema.createRecord(keyspaceAndTable, "Table " + keyspaceAndTable, ksm.getName().asInternal(), false, fields);
        this.pulsarSchema = new NativeSchemaWrapper(nativeSchema, getSchemaType());
        if (log.isInfoEnabled()) {
            log.info("schema={}", this.nativeSchema);
            for(Map.Entry<String, Schema> entry : subSchemas.entrySet()) {
                log.info("type={} schema={}", entry.getKey(), entry.getValue());
            }
        }
    }

    abstract SchemaType getSchemaType();

    @Override
    public org.apache.pulsar.client.api.Schema<byte[]> getSchema() {
        return this.pulsarSchema;
    }

    boolean isSupportedCqlType(DataType dataType) {
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
            case ProtocolConstants.DataType.CUSTOM:
                return dataType instanceof CqlVectorType;
        }
        return false;
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
            fieldSchema = new Schema.Field(fieldName, SchemaBuilder.unionOf().nullType().and().type(fieldSchema.schema()).endUnion(), null, Schema.Field.NULL_DEFAULT_VALUE);
        }
        return fieldSchema;
    }

    Schema dataTypeSchema(KeyspaceMetadata ksm, DataType dataType) {
        switch(dataType.getProtocolCode()) {
            case ProtocolConstants.DataType.INET:
            case ProtocolConstants.DataType.ASCII:
            case ProtocolConstants.DataType.VARCHAR:
                return org.apache.avro.Schema.create(Schema.Type.STRING);
            case ProtocolConstants.DataType.BLOB:
                return org.apache.avro.Schema.create(Schema.Type.BYTES);
            case ProtocolConstants.DataType.TINYINT:
            case ProtocolConstants.DataType.SMALLINT:
            case ProtocolConstants.DataType.INT:
                return org.apache.avro.Schema.create(Schema.Type.INT);
            case ProtocolConstants.DataType.BIGINT:
                return org.apache.avro.Schema.create(Schema.Type.LONG);
            case ProtocolConstants.DataType.BOOLEAN:
                return org.apache.avro.Schema.create(Schema.Type.BOOLEAN);
            case ProtocolConstants.DataType.FLOAT:
                return org.apache.avro.Schema.create(Schema.Type.FLOAT);
            case ProtocolConstants.DataType.DOUBLE:
                return org.apache.avro.Schema.create(Schema.Type.DOUBLE);
            case ProtocolConstants.DataType.TIMESTAMP:
                return CqlLogicalTypes.timestampMillisType;
            case ProtocolConstants.DataType.DATE:
                return CqlLogicalTypes.dateType;
            case ProtocolConstants.DataType.TIME:
                return CqlLogicalTypes.timeMicrosType;
            case ProtocolConstants.DataType.UDT:
                return buildUDTSchema(ksm, dataType.asCql(false, true), true);
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
            case ProtocolConstants.DataType.CUSTOM:
                if (dataType instanceof CqlVectorType) {
                    CqlVectorType vectorType = (CqlVectorType) dataType;
                    return org.apache.avro.Schema.createArray(dataTypeSchema(ksm, vectorType.getSubtype()));
                }
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
                subSchemas.put(path, dataTypeSchema(ksm, userDefinedType.getFieldTypes().get(i)));
            }
            i++;
        }
        Schema udtSchema = Schema.createRecord(typeName, "CQL type " + typeName, ksm.getName().asInternal(), false, fieldSchemas);
        subSchemas.put(typeName, udtSchema);
        return udtSchema;
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
}
