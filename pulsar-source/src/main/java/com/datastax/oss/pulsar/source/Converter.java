package com.datastax.oss.pulsar.source;


import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import org.apache.pulsar.client.api.Schema;
import com.datastax.cassandra.cdc.MutationKey;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.functions.api.Record;


public interface Converter {
    Record convert(MutationKey mutationKey, Row row, KeyspaceMetadata ksm);
}
