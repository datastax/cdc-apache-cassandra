package com.datastax.cassandra.cdc.producer.converters;

import org.apache.cassandra.config.CFMetaData;
import org.apache.pulsar.common.schema.SchemaType;

public class JsonConverter extends AbstractGenericConverter {

    public JsonConverter(CFMetaData tableMetadata) {
        super(tableMetadata, SchemaType.JSON);
    }

}
