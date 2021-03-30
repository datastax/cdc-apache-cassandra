package com.datastax.oss.pulsar.source.converters;


import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import org.apache.pulsar.common.schema.SchemaType;

import java.util.List;
import java.util.Set;

public class AvroConverter extends AbstractGenericConverter {

    public AvroConverter(KeyspaceMetadata ksm, TableMetadata tm, List<ColumnMetadata> columns) {
        super(ksm, tm, columns, SchemaType.AVRO);
    }
}
