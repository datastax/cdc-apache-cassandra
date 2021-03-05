package com.datastax.cassandra.cdc.consumer;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import org.apache.avro.Schema;
import org.apache.pulsar.client.api.schema.FieldSchemaBuilder;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.common.schema.SchemaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.util.List;
import java.util.stream.Collectors;

@Singleton
public class SchemaConverter {
    private static final Logger logger = LoggerFactory.getLogger(SchemaConverter.class);

    public RecordSchemaBuilder buildSchema(KeyspaceMetadata ksm, String tableName) {
        if (!ksm.getTable(tableName).isPresent())
            throw new IllegalArgumentException("Table metadata not found");

        TableMetadata tm = ksm.getTable(tableName).get();
        RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record(ksm.getName().toString()+"."+tableName);
        List<CqlIdentifier> pkNames = tm.getPrimaryKey().stream().map(ColumnMetadata::getName).collect(Collectors.toList());
        for(ColumnMetadata cm : tm.getColumns().values()) {
            FieldSchemaBuilder<?> fieldSchemaBuilder = null;
            switch(cm.getType().getProtocolCode()) {
                case ProtocolConstants.DataType.ASCII:
                case ProtocolConstants.DataType.VARCHAR:
                    fieldSchemaBuilder = recordSchemaBuilder.field(cm.getName().toString()).type(SchemaType.STRING);
                    break;
                case ProtocolConstants.DataType.INT:
                    fieldSchemaBuilder = recordSchemaBuilder.field(cm.getName().toString()).type(SchemaType.INT32);
                    break;
                case ProtocolConstants.DataType.BIGINT:
                    fieldSchemaBuilder = recordSchemaBuilder.field(cm.getName().toString()).type(SchemaType.INT64);
                    break;
                case ProtocolConstants.DataType.BOOLEAN:
                    fieldSchemaBuilder = recordSchemaBuilder.field(cm.getName().toString()).type(SchemaType.BOOLEAN);
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported DataType="+cm.getType().getProtocolCode());
            }
            if (pkNames.contains(cm.getName())) {
                int position = pkNames.indexOf(cm.getName());
                fieldSchemaBuilder.required().property("primary_key_order", Integer.toString(position));
            } else {
                fieldSchemaBuilder.optional();
            }
        }
        return recordSchemaBuilder;
    }

    public Schema buildAvroSchema(KeyspaceMetadata ksm, String tableName) {
        if (!ksm.getTable(tableName).isPresent())
            throw new IllegalArgumentException("Table not found");

        TableMetadata tm = ksm.getTable(tableName).get();
        org.apache.avro.SchemaBuilder.RecordBuilder<Schema> recordBuilder = org.apache.avro.SchemaBuilder
                .record(ksm.getName().toString()+"."+tableName)
                .namespace("default");
        org.apache.avro.SchemaBuilder.FieldAssembler<Schema> fieldAssembler = recordBuilder.fields();
        for(ColumnMetadata cm : tm.getColumns().values()) {
            boolean pk = false;
            for(ColumnMetadata cm2 : tm.getPrimaryKey()) {
                if (cm2.getName().equals(cm.getName())) {
                    pk = true;
                    break;
                }
            }
            switch(cm.getType().getProtocolCode()) {
                case ProtocolConstants.DataType.ASCII:
                case ProtocolConstants.DataType.VARCHAR:
                    if (pk) {
                        fieldAssembler.requiredString(cm.getName().toString());
                    } else {
                        fieldAssembler.optionalString(cm.getName().toString());
                    }
                    break;
                case ProtocolConstants.DataType.INT:
                    if (pk) {
                        fieldAssembler.requiredInt(cm.getName().toString());
                    } else {
                        fieldAssembler.optionalInt(cm.getName().toString());
                    }
                    break;
                case ProtocolConstants.DataType.BIGINT:
                    if (pk) {
                        fieldAssembler.requiredLong(cm.getName().toString());
                    } else {
                        fieldAssembler.optionalLong(cm.getName().toString());
                    }
                    break;
                case ProtocolConstants.DataType.BOOLEAN:
                    if (pk) {
                        fieldAssembler.requiredBoolean(cm.getName().toString());
                    } else {
                        fieldAssembler.optionalBoolean(cm.getName().toString());
                    }
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported DataType="+cm.getType().getProtocolCode());
            }
        }
        return fieldAssembler.endRecord();
    }
}
