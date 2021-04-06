package com.datastax.cassandra.cdc.producer.converters;

import org.apache.cassandra.schema.TableMetadata;
import org.apache.pulsar.common.schema.SchemaType;

public class AvroConverter extends AbstractGenericConverter {

    public AvroConverter(TableMetadata tableMetadata) {
        super(tableMetadata, SchemaType.AVRO);
    }

}