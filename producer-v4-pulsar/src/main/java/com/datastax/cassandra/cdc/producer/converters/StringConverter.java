package com.datastax.cassandra.cdc.producer.converters;

import com.datastax.cassandra.cdc.producer.CellData;
import com.datastax.cassandra.cdc.producer.Converter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.InetAddresses;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.pulsar.client.api.Schema;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

public class StringConverter implements Converter<String, List<CellData>, Object[]> {

    static ObjectMapper jsonMapper = new ObjectMapper();

    List<AbstractType<?>> pkTypes = new ArrayList<>();

    public StringConverter(TableMetadata tableMetadata) {
        for(ColumnMetadata cm : tableMetadata.primaryKeyColumns()) {
            pkTypes.add(cm.type);
        }
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
        Object[] values = cells.stream()
                .map(c -> c.value)
                .toArray();
        return stringify(values, values.length);
    }

    /**
     * Decode the pulsar IO internal representation.
     *
     * @param id
     * @return
     */
    @Override
    public Object[] fromConnectData(String id) throws IOException {
        Object[] values = new Object[pkTypes.size()];
        if (pkTypes.size() == 1) {
            AbstractType<?> atype = pkTypes.get(0);
            values[0] = atype.compose(fromString(atype, id));
        } else {
            Object[] elements = jsonMapper.readValue(id, Object[].class);
            for(int i=0; i < elements.length; i++) {
                AbstractType<?> atype = pkTypes.get(i);
                values[i] = atype.compose(fromString(atype, elements[i].toString()) );
            }
        }
        return values;
    }

    ByteBuffer fromString(AbstractType<?> atype, String v) throws IOException {
        if (atype instanceof BytesType)
            return ByteBuffer.wrap(Base64.getDecoder().decode(v));
        if (atype instanceof SimpleDateType)
            return SimpleDateType.instance.getSerializer().serialize(Integer.parseInt(v));
        return atype.fromString(v);
    }

    private static Object toJsonValue(Object o) {
        if (o instanceof UUID)
            return o.toString();
        if (o instanceof Date)
            return ((Date)o).getTime();
        if (o instanceof ByteBuffer) {
            // encode byte[] as Base64 encoded string
            ByteBuffer bb = ByteBufferUtil.clone((ByteBuffer)o);
            return Base64.getEncoder().encodeToString(ByteBufferUtil.getArray((ByteBuffer)o));
        }
        if (o instanceof InetAddress)
            return InetAddresses.toAddrString((InetAddress)o);
        return o;
    }

    public static String stringify(Object[] cols, int length) {
        if (cols.length == 1)
            return toJsonValue(cols[0]).toString();

        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for(int i = 0; i < length; i++) {
            if (i > 0)
                sb.append(",");
            Object val = toJsonValue(cols[i]);
            if (val instanceof String) {
                try {
                    sb.append(jsonMapper.writeValueAsString(val));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else {
                sb.append(val);
            }
        }
        return sb.append("]").toString();
    }
}
