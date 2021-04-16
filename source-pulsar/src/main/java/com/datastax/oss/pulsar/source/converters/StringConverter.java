package com.datastax.oss.pulsar.source.converters;

import com.datastax.oss.pulsar.source.Converter;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.pulsar.source.Converter;
import com.google.common.collect.ImmutableMap;
import org.apache.pulsar.client.api.Schema;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StringConverter implements Converter<String, Row, Map<String, Object>> {

    List<String> pkColumns;

    public StringConverter(KeyspaceMetadata ksm, TableMetadata tm, List<ColumnMetadata> columns) {
        this.pkColumns = tm.getPrimaryKey().stream()
                .map(c -> c.getName().toString())
                .collect(Collectors.toList());
    }

    /**
     * Return the Schema for the table primary key
     *
     * @return
     */
    @Override
    public Schema<String> getSchema() {
        return Schema.STRING;
    }

    /**
     * Return the primary key according to the Schema.
     *
     * @param row
     * @return
     */
    @Override
    public String toConnectData(Row row) {
        return row.getString(0);
    }

    /**
     * Decode the pulsar IO internal representation.
     *
     * @param value
     * @return
     */
    @Override
    public Map<String, Object> fromConnectData(String value) {
        return ImmutableMap.of(pkColumns.get(0), value);
    }
}
