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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class NativeJsonConverterKafkaTest {

    private static final CqlIdentifier KS = CqlIdentifier.fromInternal("ks1");
    private static final CqlIdentifier TABLE = CqlIdentifier.fromInternal("table1");

    private ColumnMetadata idColumn;
    private TableMetadata tableMetadata;
    private KeyspaceMetadata keyspaceMetadata;

    @BeforeEach
    void setUp() {
        idColumn = new DefaultColumnMetadata(KS, TABLE, CqlIdentifier.fromInternal("id"), DataTypes.INT, false);

        tableMetadata = mock(TableMetadata.class);
        when(tableMetadata.getKeyspace()).thenReturn(KS);
        when(tableMetadata.getName()).thenReturn(TABLE);
        when(tableMetadata.getPartitionKey()).thenReturn(List.of(idColumn));
        when(tableMetadata.getPrimaryKey()).thenReturn(List.of(idColumn));

        keyspaceMetadata = mock(KeyspaceMetadata.class);
        when(keyspaceMetadata.getName()).thenReturn(KS);
    }

    @Test
    void should_decode_avro_encoded_primary_key_to_json_bytes() throws Exception {
        NativeJsonConverter converter = new NativeJsonConverter(keyspaceMetadata, tableMetadata, List.of(idColumn));

        GenericData.Record record = new GenericData.Record(converter.nativeSchema);
        record.put("id", 7);

        byte[] jsonBytes = converter.fromConnectData(record);

        JsonNode node = new ObjectMapper().readTree(jsonBytes);
        assertThat(node.get("id").asInt()).isEqualTo(7);
    }
}
