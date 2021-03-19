package com.datastax.cassandra.cdc.producer.converters;

import com.datastax.cassandra.cdc.converter.Converter;
import com.datastax.cassandra.cdc.producer.CellData;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.*;
import org.apache.pulsar.client.impl.schema.generic.GenericSchemaImpl;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractGenericConverter implements Converter<GenericRecord, List<CellData>, Object[]> {

    GenericSchema<GenericRecord> schema;
    TableMetadata tableMetadata;

    @SuppressWarnings("unchecked")
    public AbstractGenericConverter(TableMetadata tableMetadata, SchemaType schemaType) {
        RecordSchemaBuilder recordSchemaBuilder =
                SchemaBuilder.record(tableMetadata.keyspace+"."+tableMetadata.name);
        for(ColumnMetadata cm : tableMetadata.primaryKeyColumns()) {
            if (cm.type instanceof UTF8Type) {
                recordSchemaBuilder.field(cm.name.toString()).type(SchemaType.STRING);
            } else if (cm.type instanceof Int32Type) {
                recordSchemaBuilder.field(cm.name.toString()).type(SchemaType.INT32);
            } else if (cm.type instanceof IntegerType) {
                recordSchemaBuilder.field(cm.name.toString()).type(SchemaType.INT64);
            } else if (cm.type instanceof BooleanType) {
                recordSchemaBuilder.field(cm.name.toString()).type(SchemaType.BOOLEAN);
            } else if (cm.type instanceof BooleanType) {
                recordSchemaBuilder.field(cm.name.toString()).type(SchemaType.BOOLEAN);
            }
        }
        SchemaInfo schemaInfo = recordSchemaBuilder.build(schemaType);
        this.schema = GenericSchemaImpl.of(schemaInfo);
        this.tableMetadata = tableMetadata;
    }

    @Override
    public Schema<GenericRecord> getSchema() {
        return schema;
    }

    /**
     * Convert PK to generic record
     *
     * @param cells
     * @return
     */
    @Override
    public GenericRecord toConnectData(List<CellData> cells) {
        GenericRecordBuilder genericRecordBuilder = schema.newRecordBuilder();
        for(CellData cell : cells) {
            genericRecordBuilder.set(cell.name, cell.value);
        }
        return genericRecordBuilder.build();
    }

    /**
     * Convert GenericRecord to primary key column values.
     * @param genericRecord
     * @return
     */
    @Override
    public Object[] fromConnectData(GenericRecord genericRecord) {
        List<Object> pk = new ArrayList<>();
        for(ColumnMetadata cm : tableMetadata.primaryKeyColumns()) {
            pk.add(genericRecord.getField(cm.name.toString()));
        }
        return pk.toArray(new Object[pk.size()]);
    }
}
