package com.datastax.cassandra.cdc;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;

import java.sql.Timestamp;

public class CDCSchema {

    public static final Schema<KeyValue<PrimaryKey, Timestamp>> kvSchema = Schema.KeyValue(
            JSONSchema.of(PrimaryKey.class),
            Schema.TIMESTAMP,
            KeyValueEncodingType.SEPARATED
    );

}
