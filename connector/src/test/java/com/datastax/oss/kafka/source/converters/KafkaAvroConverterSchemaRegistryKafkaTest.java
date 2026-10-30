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
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultColumnMetadata;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.generic.GenericDatumReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KafkaAvroConverterSchemaRegistryKafkaTest {

    private static final CqlIdentifier KS = CqlIdentifier.fromInternal("ks1");
    private static final CqlIdentifier TABLE = CqlIdentifier.fromInternal("table1");
    private static final String OUTPUT_TOPIC = "data-ks1.table1";

    private KafkaAvroConverter converter;
    private MockSchemaRegistryClient schemaRegistryClient;

    @BeforeEach
    void setUp() {
        ColumnMetadata idColumn = new DefaultColumnMetadata(KS, TABLE, CqlIdentifier.fromInternal("id"), DataTypes.INT, false);
        TableMetadata tableMetadata = mock(TableMetadata.class);
        when(tableMetadata.getKeyspace()).thenReturn(KS);
        when(tableMetadata.getName()).thenReturn(TABLE);
        when(tableMetadata.getPartitionKey()).thenReturn(List.of(idColumn));
        when(tableMetadata.getPrimaryKey()).thenReturn(List.of(idColumn));
        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
        when(keyspaceMetadata.getName()).thenReturn(KS);

        converter = new KafkaAvroConverter(keyspaceMetadata, tableMetadata, List.of(idColumn));

        schemaRegistryClient = new MockSchemaRegistryClient();
        KafkaAvroSerializer serializer = new KafkaAvroSerializer(schemaRegistryClient,
                Map.of("schema.registry.url", "mock://schema-registry-not-used"));
        converter.enableSchemaRegistry(serializer, OUTPUT_TOPIC);
    }

    private Row mockRow(int idValue) {
        ColumnDefinition idColumnDefinition = mock(ColumnDefinition.class);
        CqlIdentifier idName = CqlIdentifier.fromInternal("id");
        when(idColumnDefinition.getName()).thenReturn(idName);
        when(idColumnDefinition.getType()).thenReturn(DataTypes.INT);

        ColumnDefinitions columnDefinitions = mock(ColumnDefinitions.class);
        when(columnDefinitions.iterator()).thenAnswer(inv -> List.of(idColumnDefinition).iterator());

        Row row = mock(Row.class);
        when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
        when(row.isNull(idName)).thenReturn(false);
        when(row.getInt(idName)).thenReturn(idValue);
        return row;
    }

    @Test
    void should_publish_in_confluent_wire_format_and_register_schema() throws Exception {
        byte[] bytes = converter.toConnectData(mockRow(42));

        // Confluent wire format: magic byte 0x0, then a 4-byte big-endian schema id, then the
        // Avro binary payload.
        assertThat(bytes[0]).isEqualTo((byte) 0);
        int schemaId = ByteBuffer.wrap(bytes, 1, 4).getInt();

        SchemaMetadata registered = schemaRegistryClient.getSchemaMetadata(OUTPUT_TOPIC + "-value", 1);
        assertThat(registered.getId()).isEqualTo(schemaId);

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, 5, bytes.length - 5, null);
        GenericRecord decoded = new GenericDatumReader<GenericRecord>(converter.nativeSchema).read(null, decoder);
        assertThat(decoded.get("id")).isEqualTo(42);
    }

    @Test
    void should_reuse_the_same_schema_id_across_rows() throws Exception {
        byte[] first = converter.toConnectData(mockRow(1));
        byte[] second = converter.toConnectData(mockRow(2));

        assertThat(ByteBuffer.wrap(first, 1, 4).getInt()).isEqualTo(ByteBuffer.wrap(second, 1, 4).getInt());
        assertThat(schemaRegistryClient.getAllSubjects()).containsExactly(OUTPUT_TOPIC + "-value");
    }

    @Test
    void should_fall_back_to_raw_avro_bytes_when_schema_registry_not_enabled() {
        ColumnMetadata idColumn = new DefaultColumnMetadata(KS, TABLE, CqlIdentifier.fromInternal("id"), DataTypes.INT, false);
        TableMetadata tableMetadata = mock(TableMetadata.class);
        when(tableMetadata.getKeyspace()).thenReturn(KS);
        when(tableMetadata.getName()).thenReturn(TABLE);
        when(tableMetadata.getPartitionKey()).thenReturn(List.of(idColumn));
        when(tableMetadata.getPrimaryKey()).thenReturn(List.of(idColumn));
        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
        when(keyspaceMetadata.getName()).thenReturn(KS);
        KafkaAvroConverter plainConverter = new KafkaAvroConverter(keyspaceMetadata, tableMetadata, List.of(idColumn));

        byte[] bytes = plainConverter.toConnectData(mockRow(42));

        // No Confluent header: raw Avro-encoded int 42 zigzag-encodes to a single byte, 0x54.
        assertThat(bytes).isEqualTo(new byte[]{0x54});
    }
}
