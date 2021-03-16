package com.datastax.oss.pulsar.source;


import com.datastax.cassandra.cdc.MutationKey;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.functions.api.Record;

public class JsonConverter implements Converter {

    public Schema<KeyValue<String, Object>> getSchema() {
        return Schema.KeyValue(Schema.STRING, Schema.JSON(Object.class), KeyValueEncodingType.SEPARATED);
    }

    @Override
    public Record convert(MutationKey mutationKey, Row row, KeyspaceMetadata ksm) {
        return null;
    }
}
