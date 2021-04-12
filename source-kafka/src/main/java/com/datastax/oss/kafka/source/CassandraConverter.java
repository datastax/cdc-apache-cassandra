package com.datastax.oss.kafka.source;

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
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.*;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class CassandraConverter {

    final Schema schema;
    final Collection<ColumnMetadata> columns;
    final Map<String, Schema> udtSchemas = new HashMap<>();

    public CassandraConverter(KeyspaceMetadata ksm, TableMetadata tm, Collection<ColumnMetadata> columns) {
        SchemaBuilder schemaBuilder = SchemaBuilder.struct()
                .name(ksm.getName()+ "." + tm.getName())
                .doc("Cassandra table " + ksm.getName()+ "." + tm.getName())
                .optional();
        for(ColumnMetadata cm : columns) {
            addFieldSchema(schemaBuilder, ksm, cm.getName().toString(), cm.getType());
        }
        this.schema = schemaBuilder.build();
        this.columns = columns;
        log.info("schema={}", this.schema);
    }

    private SchemaBuilder addFieldSchema(SchemaBuilder schemaBuilder,
                                 KeyspaceMetadata ksm,
                                 String fieldName,
                                 DataType dataType) {
        switch(dataType.getProtocolCode()) {
            case ProtocolConstants.DataType.UUID:
            case ProtocolConstants.DataType.TIMEUUID:
            case ProtocolConstants.DataType.ASCII:
            case ProtocolConstants.DataType.VARCHAR:
                schemaBuilder.field(fieldName, Schema.OPTIONAL_STRING_SCHEMA);
                break;
            case ProtocolConstants.DataType.BLOB:
                schemaBuilder.field(fieldName, Schema.OPTIONAL_BYTES_SCHEMA);
                break;
            case ProtocolConstants.DataType.TINYINT:
                schemaBuilder.field(fieldName, Schema.OPTIONAL_INT8_SCHEMA);
                break;
            case ProtocolConstants.DataType.SMALLINT:
                schemaBuilder.field(fieldName, Schema.OPTIONAL_INT16_SCHEMA);
                break;
            case ProtocolConstants.DataType.INT:
                schemaBuilder.field(fieldName, Schema.OPTIONAL_INT32_SCHEMA);
                break;
            case ProtocolConstants.DataType.INET:
            case ProtocolConstants.DataType.DURATION:
            case ProtocolConstants.DataType.BIGINT:
                schemaBuilder.field(fieldName, Schema.OPTIONAL_INT64_SCHEMA);
                break;
            case ProtocolConstants.DataType.BOOLEAN:
                schemaBuilder.field(fieldName, Schema.OPTIONAL_BOOLEAN_SCHEMA);
                break;
            case ProtocolConstants.DataType.FLOAT:
                schemaBuilder.field(fieldName, Schema.OPTIONAL_FLOAT32_SCHEMA);
                break;
            case ProtocolConstants.DataType.DOUBLE:
                schemaBuilder.field(fieldName, Schema.OPTIONAL_FLOAT64_SCHEMA);
                break;
            case ProtocolConstants.DataType.DATE:
                schemaBuilder.field(fieldName, Date.builder().build()).optional();
                break;
            case ProtocolConstants.DataType.TIMESTAMP:
                schemaBuilder.field(fieldName, Timestamp.builder().build()).optional();
                break;
            case ProtocolConstants.DataType.TIME:
                schemaBuilder.field(fieldName, Time.builder().build()).optional();
                break;
            case ProtocolConstants.DataType.UDT:
                schemaBuilder
                        .field(fieldName, buildUDTSchema(ksm, dataType.asCql(false, true)))
                        .optional();
                break;
            default:
                throw new UnsupportedOperationException("Unsupported DataType=" + dataType.getProtocolCode());
        }
        return schemaBuilder;
    }

    private Schema buildUDTSchema(KeyspaceMetadata ksm, String typeName) {
        UserDefinedType userDefinedType = ksm.getUserDefinedType(CqlIdentifier.fromCql(typeName.substring(typeName.indexOf(".") + 1))).get();
        if (userDefinedType == null) {
            throw new IllegalStateException("UDT " + typeName + " not found");
        }
        SchemaBuilder udtSchemaBuilder = SchemaBuilder.struct()
                .name(ksm.getName()+ "." + typeName)
                .doc("Cassandra type " + ksm.getName()+ "." + typeName);
        int i = 0;
        for(CqlIdentifier field : userDefinedType.getFieldNames()) {
            addFieldSchema(udtSchemaBuilder, ksm, field.toString(), userDefinedType.getFieldTypes().get(i++));
        }
        Schema udtSchema = udtSchemaBuilder.build();
        udtSchemas.put(typeName, udtSchema);
        return udtSchemaBuilder;
    }

    Schema getSchema() {
        return this.schema;
    }

    Struct buildStruct(Row row) {
        Struct struct = new Struct(this.schema);
        for(ColumnDefinition cm : row.getColumnDefinitions()) {
            switch(cm.getType().getProtocolCode()) {
                case ProtocolConstants.DataType.UUID:
                case ProtocolConstants.DataType.TIMEUUID:
                    struct.put(cm.getName().toString(), row.getUuid(cm.getName()).toString());
                    break;
                case ProtocolConstants.DataType.ASCII:
                case ProtocolConstants.DataType.VARCHAR:
                    struct.put(cm.getName().toString(), row.getString(cm.getName()));
                    break;
                case ProtocolConstants.DataType.TINYINT:
                    struct.put(cm.getName().toString(), row.getByte(cm.getName()));
                    break;
                case ProtocolConstants.DataType.SMALLINT:
                    struct.put(cm.getName().toString(), row.getShort(cm.getName()));
                    break;
                case ProtocolConstants.DataType.INT:
                    struct.put(cm.getName().toString(), row.getInt(cm.getName()));
                    break;
                case ProtocolConstants.DataType.INET:
                case ProtocolConstants.DataType.BIGINT:
                    struct.put(cm.getName().toString(), row.getLong(cm.getName()));
                    break;
                case ProtocolConstants.DataType.DOUBLE:
                    struct.put(cm.getName().toString(), row.getDouble(cm.getName()));
                    break;
                case ProtocolConstants.DataType.FLOAT:
                    struct.put(cm.getName().toString(), row.getFloat(cm.getName()));
                    break;
                case ProtocolConstants.DataType.BOOLEAN:
                    struct.put(cm.getName().toString(), row.getBoolean(cm.getName()));
                    break;
                case ProtocolConstants.DataType.DATE:
                    struct.put(cm.getName().toString(), row.getLocalDate(cm.getName()));
                    break;
                case ProtocolConstants.DataType.DURATION:
                    struct.put(cm.getName().toString(), row.getCqlDuration(cm.getName()));
                    break;
                case ProtocolConstants.DataType.TIME:
                    struct.put(cm.getName().toString(), row.getLocalTime(cm.getName()));
                    break;
                case ProtocolConstants.DataType.UDT:
                    struct.put(cm.getName().toString(), buildUDTValue(row.getUdtValue(cm.getName())));
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported DataType=" + cm.getType().getProtocolCode());
            }
        }
        return struct;
    }

    Struct buildUDTValue(UdtValue udtValue) {
        String typeName = udtValue.getType().getKeyspace() + "." + udtValue.getType().getName().asCql(true);
        Schema schema = udtSchemas.get(typeName);
        assert schema != null : "Schema not found for UDT=" + typeName;
        Struct struct = new Struct(schema);
        for(CqlIdentifier field : udtValue.getType().getFieldNames()) {
            DataType dataType = udtValue.getType(field);
            switch(dataType.getProtocolCode()) {
                case ProtocolConstants.DataType.UUID:
                case ProtocolConstants.DataType.TIMEUUID:
                    struct.put(field.toString(), udtValue.getUuid(field).toString());
                    break;
                case ProtocolConstants.DataType.ASCII:
                case ProtocolConstants.DataType.VARCHAR:
                    struct.put(field.toString(), udtValue.getString(field));
                    break;
                case ProtocolConstants.DataType.TINYINT:
                    struct.put(field.toString(), udtValue.getByte(field));
                    break;
                case ProtocolConstants.DataType.SMALLINT:
                    struct.put(field.toString(), udtValue.getShort(field));
                    break;
                case ProtocolConstants.DataType.INT:
                    struct.put(field.toString(), udtValue.getInt(field));
                    break;
                case ProtocolConstants.DataType.INET:
                case ProtocolConstants.DataType.BIGINT:
                    struct.put(field.toString(), udtValue.getLong(field));
                    break;
                case ProtocolConstants.DataType.DOUBLE:
                    struct.put(field.toString(), udtValue.getDouble(field));
                    break;
                case ProtocolConstants.DataType.FLOAT:
                    struct.put(field.toString(), udtValue.getFloat(field));
                    break;
                case ProtocolConstants.DataType.BOOLEAN:
                    struct.put(field.toString(), udtValue.getBoolean(field));
                    break;
                case ProtocolConstants.DataType.DATE:
                    struct.put(field.toString(), udtValue.getLocalDate(field));
                    break;
                case ProtocolConstants.DataType.DURATION:
                    struct.put(field.toString(), udtValue.getCqlDuration(field));
                    break;
                case ProtocolConstants.DataType.TIME:
                    struct.put(field.toString(), udtValue.getLocalTime(field));
                    break;
                case ProtocolConstants.DataType.UDT:
                    struct.put(field.toString(), buildUDTValue(udtValue.getUdtValue(field)));
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported field="+field.toString() + " DataType=" + dataType.getProtocolCode());
            }
        }
        return struct;
    }
}
