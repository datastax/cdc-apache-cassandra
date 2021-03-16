package com.datastax.oss.pulsar.source;

import com.datastax.cassandra.cdc.MutationKey;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.functions.api.Record;
import org.json.JSONArray;

import java.util.List;
import java.util.Optional;

@Slf4j
public class JsonStringConverter implements Converter {

    @Override
    public Record convert(MutationKey mutationKey, Row row, KeyspaceMetadata ksm) {

        final String msgKey;
        List<ColumnMetadata> pkColumns = ksm.getTable(mutationKey.getTable()).get().getPrimaryKey();
        if (pkColumns.size() > 1) {
            JSONArray ja = new JSONArray();
            int i = 0;
            for(ColumnMetadata cm : pkColumns)
                ja.put(mutationKey.getPkColumns()[i++]);
            msgKey = ja.toString();
        } else {
            msgKey = mutationKey.getPkColumns()[0].toString();
        }

        String jsonString = row == null ? null :row.getString(0);
        log.debug("key={} value={}", msgKey, jsonString);
        final KeyValue<String, String> keyValue =  new KeyValue<String,String>(msgKey, jsonString);

        return new Record() {
            @Override
            public Schema getSchema() {
                return Schema.KeyValue(Schema.STRING, Schema.STRING, KeyValueEncodingType.SEPARATED);
            }

            @Override
            public Optional<String> getKey() {
                return Optional.ofNullable(keyValue.getKey());
            }

            @Override
            public KeyValue getValue() {
                return keyValue;
            }
        };
    }
}
