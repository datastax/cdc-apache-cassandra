package com.datastax.cassandra.cdc.producer.converters;

import com.datastax.cassandra.cdc.converter.Converter;
import com.datastax.cassandra.cdc.producer.CellData;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.pulsar.client.api.Schema;

import java.util.List;

public class StringConverter implements Converter<String, List<CellData>, Object[]> {

    public StringConverter(TableMetadata tableMetadata) {
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
     * @param cells
     * @return
     */
    @Override
    public String toConnectData(List<CellData> cells) {
        if (cells.size() == 1) {
            return cells.get(0).value.toString();
        } else {
            StringBuffer sb = new StringBuffer("[");
            for(CellData cell : cells) {
                if (sb.length() > 1)
                    sb.append(",");
                sb.append(cell.value.toString());
            }
            return sb.append("]").toString();
        }
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
