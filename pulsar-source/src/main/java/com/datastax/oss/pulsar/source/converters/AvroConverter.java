package com.datastax.oss.pulsar.source.converters;


import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import org.apache.pulsar.common.schema.SchemaType;

import java.util.Set;

public class AvroConverter extends AbstractGenericConverter {

    public AvroConverter(TableMetadata tableMetadata, Set<String> columns) {
        super(tableMetadata, columns, SchemaType.AVRO);
    }

}
