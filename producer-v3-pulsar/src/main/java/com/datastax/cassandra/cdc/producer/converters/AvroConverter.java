package com.datastax.cassandra.cdc.producer.converters;

import org.apache.cassandra.config.CFMetaData;
import org.apache.pulsar.common.schema.SchemaType;

public class AvroConverter extends AbstractGenericConverter {

    public AvroConverter(CFMetaData tableMetadata) {
        super(tableMetadata, SchemaType.AVRO);
    }

}
