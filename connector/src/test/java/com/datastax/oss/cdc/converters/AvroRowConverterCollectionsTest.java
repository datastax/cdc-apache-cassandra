/**
 * Copyright DataStax, Inc 2021.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.cdc.converters;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultColumnMetadata;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Covers the collection, vector and tuple paths of {@link AvroRowConverter}: the sub-schemas the
 * constructor registers and the values {@code buildGenericRecord} produces for each kind.
 */
class AvroRowConverterCollectionsTest {

    private static final CqlIdentifier KS = CqlIdentifier.fromInternal("ks1");
    private static final CqlIdentifier TABLE = CqlIdentifier.fromInternal("table1");
    private static final TupleType TUPLE_TYPE = DataTypes.tupleOf(DataTypes.INT, DataTypes.TEXT);

    private final Map<String, DataType> columnTypes = new LinkedHashMap<>();
    private AvroRowConverter converter;

    private static ColumnMetadata column(String name, DataType type) {
        return new DefaultColumnMetadata(KS, TABLE, CqlIdentifier.fromInternal(name), type, false);
    }

    @BeforeEach
    void setUp() {
        columnTypes.put("id", DataTypes.INT);
        columnTypes.put("l", DataTypes.listOf(DataTypes.INT));
        columnTypes.put("s", DataTypes.setOf(DataTypes.TEXT));
        columnTypes.put("m", DataTypes.mapOf(DataTypes.TEXT, DataTypes.INT));
        columnTypes.put("v", DataTypes.vectorOf(DataTypes.FLOAT, 3));
        columnTypes.put("t", TUPLE_TYPE);

        List<ColumnMetadata> columns = new ArrayList<>();
        columnTypes.forEach((name, type) -> columns.add(column(name, type)));

        TableMetadata tableMetadata = mock(TableMetadata.class);
        when(tableMetadata.getKeyspace()).thenReturn(KS);
        when(tableMetadata.getName()).thenReturn(TABLE);
        when(tableMetadata.getPartitionKey()).thenReturn(Collections.singletonList(columns.get(0)));
        when(tableMetadata.getPrimaryKey()).thenReturn(Collections.singletonList(columns.get(0)));
        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
        when(keyspaceMetadata.getName()).thenReturn(KS);

        converter = new AvroRowConverter(keyspaceMetadata, tableMetadata, columns);
    }

    private Row mockRow(Map<String, Boolean> nulls) {
        List<ColumnDefinition> definitions = new ArrayList<>();
        Row row = mock(Row.class);
        columnTypes.forEach((name, type) -> {
            ColumnDefinition definition = mock(ColumnDefinition.class);
            CqlIdentifier id = CqlIdentifier.fromInternal(name);
            when(definition.getName()).thenReturn(id);
            when(definition.getType()).thenReturn(type);
            definitions.add(definition);
            when(row.isNull(id)).thenReturn(nulls.getOrDefault(name, false));
        });
        ColumnDefinitions columnDefinitions = mock(ColumnDefinitions.class);
        when(columnDefinitions.iterator()).thenAnswer(inv -> definitions.iterator());
        when(row.getColumnDefinitions()).thenReturn(columnDefinitions);

        when(row.getInt(CqlIdentifier.fromInternal("id"))).thenReturn(7);
        when(row.getList(eq("l"), any())).thenReturn(Arrays.asList(1, 2, 3));
        when(row.getSet(eq("s"), any())).thenReturn(new HashSet<>(Arrays.asList("a", "b")));
        Map<String, Integer> map = new HashMap<>();
        map.put("k1", 10);
        map.put("k2", 20);
        when(row.getMap(eq("m"), any(), any())).thenReturn((Map) map);
        when(row.getVector(eq("v"), any())).thenReturn((CqlVector) CqlVector.newInstance(1.5f, 2.5f, 3.5f));
        TupleValue tuple = TUPLE_TYPE.newValue(42, "x");
        when(row.getTupleValue(CqlIdentifier.fromInternal("t"))).thenReturn(tuple);
        return row;
    }

    @Test
    void registersSubSchemasForCollectionsVectorsAndTuples() {
        assertNotNull(converter.subSchemas.get("l"));
        assertNotNull(converter.subSchemas.get("s"));
        assertNotNull(converter.subSchemas.get("m"));
        assertNotNull(converter.subSchemas.get("v"));
        assertNotNull(converter.subSchemas.get("t"));
        assertNull(converter.subSchemas.get("id"), "a plain int column needs no sub-schema");
        assertEquals(6, converter.nativeSchema.getFields().size());
    }

    @Test
    void buildsRecordWithEveryCollectionKind() {
        GenericRecord rec = converter.buildGenericRecord(mockRow(Collections.emptyMap()));

        assertEquals(7, rec.get("id"));
        assertEquals(Arrays.asList(1, 2, 3), new ArrayList<>((GenericArray<?>) rec.get("l")));
        assertEquals(new HashSet<>(Arrays.asList("a", "b")), new HashSet<>((GenericArray<?>) rec.get("s")));
        Map<?, ?> m = (Map<?, ?>) rec.get("m");
        assertEquals(10, m.get("k1"));
        assertEquals(20, m.get("k2"));
        assertEquals(Arrays.asList(1.5f, 2.5f, 3.5f), new ArrayList<>((GenericArray<?>) rec.get("v")));
        GenericRecord tuple = (GenericRecord) rec.get("t");
        assertEquals(42, tuple.get(0));
        assertEquals("x", tuple.get(1).toString());
    }

    @Test
    void serializesTheRecord() {
        byte[] bytes = converter.toConnectData(mockRow(Collections.emptyMap()));
        assertTrue(bytes.length > 0);
    }

    @Test
    void leavesNullColumnsUnset() {
        Map<String, Boolean> nulls = new HashMap<>();
        nulls.put("l", true);
        nulls.put("v", true);
        GenericRecord rec = converter.buildGenericRecord(mockRow(nulls));
        assertNull(rec.get("l"));
        assertNull(rec.get("v"));
        assertEquals(7, rec.get("id"));
    }
}
