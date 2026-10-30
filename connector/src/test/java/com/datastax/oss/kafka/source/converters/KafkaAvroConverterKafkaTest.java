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
package com.datastax.oss.kafka.source.converters;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultColumnMetadata;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KafkaAvroConverterKafkaTest {

    private static final CqlIdentifier KS = CqlIdentifier.fromInternal("ks1");
    private static final CqlIdentifier TABLE = CqlIdentifier.fromInternal("table1");

    private ColumnMetadata idColumn;
    private ColumnMetadata nameColumn;
    private ColumnMetadata createdAtColumn;
    private TableMetadata tableMetadata;
    private KeyspaceMetadata keyspaceMetadata;

    @BeforeEach
    void setUp() {
        idColumn = new DefaultColumnMetadata(KS, TABLE, CqlIdentifier.fromInternal("id"), DataTypes.INT, false);
        nameColumn = new DefaultColumnMetadata(KS, TABLE, CqlIdentifier.fromInternal("name"), DataTypes.TEXT, false);
        createdAtColumn = new DefaultColumnMetadata(KS, TABLE, CqlIdentifier.fromInternal("created_at"), DataTypes.TIMESTAMP, false);

        tableMetadata = mock(TableMetadata.class);
        when(tableMetadata.getKeyspace()).thenReturn(KS);
        when(tableMetadata.getName()).thenReturn(TABLE);
        when(tableMetadata.getPartitionKey()).thenReturn(List.of(idColumn));
        when(tableMetadata.getPrimaryKey()).thenReturn(List.of(idColumn));

        keyspaceMetadata = mock(KeyspaceMetadata.class);
        when(keyspaceMetadata.getName()).thenReturn(KS);
    }

    @Test
    void should_build_schema_with_nullable_non_pk_fields() {
        KafkaAvroConverter converter = new KafkaAvroConverter(keyspaceMetadata, tableMetadata,
                Arrays.asList(idColumn, nameColumn, createdAtColumn));

        Schema schema = converter.nativeSchema;
        assertThat(schema.getFields()).extracting(Schema.Field::name)
                .containsExactly("id", "name", "created_at");

        // partition key column is not wrapped in a nullable union
        assertThat(schema.getField("id").schema().getType()).isEqualTo(Schema.Type.INT);

        // non-key columns are wrapped in a nullable union
        assertThat(schema.getField("name").schema().getType()).isEqualTo(Schema.Type.UNION);
        assertThat(schema.getField("name").schema().getTypes())
                .extracting(Schema::getType)
                .contains(Schema.Type.NULL);
    }

    @Test
    void should_round_trip_primary_key_via_from_connect_data() {
        KafkaAvroConverter mutationKeyConverter = new KafkaAvroConverter(keyspaceMetadata, tableMetadata,
                List.of(idColumn));

        GenericData.Record record = new GenericData.Record(mutationKeyConverter.nativeSchema);
        record.put("id", 42);

        List<Object> pk = mutationKeyConverter.fromConnectData(record);

        assertThat(pk).containsExactly(42);
    }

    @Test
    void should_convert_timestamp_pk_from_epoch_millis_to_instant() {
        ColumnMetadata tsPk = new DefaultColumnMetadata(KS, TABLE, CqlIdentifier.fromInternal("created_at"), DataTypes.TIMESTAMP, false);
        TableMetadata tsTable = mock(TableMetadata.class);
        when(tsTable.getKeyspace()).thenReturn(KS);
        when(tsTable.getName()).thenReturn(TABLE);
        when(tsTable.getPartitionKey()).thenReturn(List.of(tsPk));
        when(tsTable.getPrimaryKey()).thenReturn(List.of(tsPk));

        KafkaAvroConverter mutationKeyConverter = new KafkaAvroConverter(keyspaceMetadata, tsTable, List.of(tsPk));

        Instant now = Instant.ofEpochMilli(1_700_000_000_000L);
        GenericData.Record record = new GenericData.Record(mutationKeyConverter.nativeSchema);
        record.put("created_at", now.toEpochMilli());

        List<Object> pk = mutationKeyConverter.fromConnectData(record);

        assertThat(pk).containsExactly(now);
    }
}
