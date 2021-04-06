package com.datastax.oss.pulsar.source.converters;

import com.datastax.oss.pulsar.source.Converter;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.pulsar.source.Converter;
import org.apache.pulsar.client.api.Schema;

import java.util.List;

public class StringConverter implements Converter<String, Row, Object[]> {

    public StringConverter(KeyspaceMetadata ksm, TableMetadata tm, List<ColumnMetadata> columns) {
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
    public Object[] fromConnectData(String value) {
        return new Object[] { value };
    }
}
