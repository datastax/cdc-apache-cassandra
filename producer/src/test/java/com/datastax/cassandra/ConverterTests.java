package com.datastax.cassandra;

import com.datastax.cassandra.cdc.producer.CellData;
import com.datastax.cassandra.cdc.producer.converters.StringConverter;
import com.google.common.collect.Lists;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.TableMetadata;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConverterTests {

    @Test
    public void testSingleStringConverter() throws IOException {
        TableMetadata tableMetadata = TableMetadata.builder("ks1","table1")
                .addPartitionKeyColumn("id", UTF8Type.instance)
                .build();

        StringConverter stringConverter = new StringConverter(tableMetadata);
        CellData cellData = new CellData("id","1", null, CellData.ColumnType.PARTITION);
        String id = stringConverter.toConnectData(Lists.newArrayList(cellData));
        assertEquals("1", id);
        Object[] pkColumns = stringConverter.fromConnectData(id);
        assertEquals(1L, pkColumns.length);
        assertEquals("1", pkColumns[0]);
    }

    @Test
    public void testCompoundStringConverter() throws IOException {
        TableMetadata tableMetadata = TableMetadata.builder("ks1","table1")
                .addPartitionKeyColumn("id", UTF8Type.instance)
                .addClusteringColumn("a", LongType.instance)
                .build();

        StringConverter stringConverter = new StringConverter(tableMetadata);
        CellData cellData0 = new CellData("id","1", null, CellData.ColumnType.PARTITION);
        CellData cellData1 = new CellData("a",1L, null, CellData.ColumnType.CLUSTERING);
        String id = stringConverter.toConnectData(Lists.newArrayList(cellData0, cellData1));
        assertEquals("[\"1\",1]", id);
        Object[] pkColumns = stringConverter.fromConnectData(id);
        assertEquals(2L, pkColumns.length);
        assertEquals("1", pkColumns[0]);
        assertEquals(1L, pkColumns[1]);
    }
}
