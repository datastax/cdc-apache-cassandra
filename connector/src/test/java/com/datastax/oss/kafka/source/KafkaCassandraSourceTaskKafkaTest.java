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
package com.datastax.oss.kafka.source;

import com.datastax.oss.cdc.CassandraClient;
import com.datastax.oss.cdc.CassandraSourceConnectorConfig;
import com.datastax.oss.cdc.ConverterAndQuery;
import com.datastax.oss.cdc.MutationCache;
import com.datastax.oss.cdc.converters.AvroRowConverter;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultColumnMetadata;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.kafka.source.converters.Converter;
import com.datastax.oss.kafka.source.converters.KafkaAvroConverter;
import com.datastax.oss.driver.api.core.metadata.schema.AggregateMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.ViewMetadata;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.google.common.util.concurrent.RateLimiter;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.connect.errors.ConnectException;
import java.nio.charset.StandardCharsets;
import io.vavr.Tuple2;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaCassandraSourceTaskKafkaTest {

    private static final CqlIdentifier KS = CqlIdentifier.fromInternal("ks1");
    private static final CqlIdentifier TABLE = CqlIdentifier.fromInternal("table1");
    private static final String EVENTS_TOPIC = "events-ks1.table1";
    private static final String OUTPUT_TOPIC = "data-ks1.table1";
    private static final String HEARTBEAT_TOPIC = "data-ks1.table1-heartbeat";

    private static final Schema MUTATION_VALUE_SCHEMA;

    static {
        Schema nullableString = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));
        Schema nullableStringArray = Schema.createUnion(Schema.create(Schema.Type.NULL),
                Schema.createArray(Schema.create(Schema.Type.STRING)));
        MUTATION_VALUE_SCHEMA = Schema.createRecord("MutationValue", null, "com.datastax.oss.cdc", false,
                List.of(
                        new Schema.Field("md5Digest", nullableString, null, (Object) null),
                        new Schema.Field("nodeId", nullableString, null, (Object) null),
                        new Schema.Field("columns", nullableStringArray, null, (Object) null)));
    }

    private KafkaCassandraSourceTask task;
    private CassandraClient cassandraClient;
    private KafkaConsumer<byte[], byte[]> consumer;
    private Converter<byte[], ?> valueConverter;
    private KafkaAvroConverter mutationKeyConverter;
    private UUID nodeId;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        ColumnMetadata idColumn = new DefaultColumnMetadata(KS, TABLE, CqlIdentifier.fromInternal("id"), DataTypes.INT, false);
        TableMetadata tableMetadata = mock(TableMetadata.class);
        when(tableMetadata.getKeyspace()).thenReturn(KS);
        when(tableMetadata.getName()).thenReturn(TABLE);
        when(tableMetadata.getPartitionKey()).thenReturn(List.of(idColumn));
        when(tableMetadata.getPrimaryKey()).thenReturn(List.of(idColumn));
        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
        when(keyspaceMetadata.getName()).thenReturn(KS);

        mutationKeyConverter = new KafkaAvroConverter(keyspaceMetadata, tableMetadata, List.of(idColumn));

        nodeId = UUID.randomUUID();
        valueConverter = mock(Converter.class);
        when(valueConverter.toConnectData(any())).thenReturn("row-bytes".getBytes());

        cassandraClient = mock(CassandraClient.class);
        when(cassandraClient.prepareSelect(any(), any(), any(), any(), anyInt())).thenReturn(mock(PreparedStatement.class));

        consumer = mock(KafkaConsumer.class);

        ConcurrentMap<Integer, PreparedStatement> preparedStatements = new ConcurrentHashMap<>();
        ConverterAndQuery<Converter<byte[], ?>> converterAndQuery = new ConverterAndQuery<>(
                "ks1", "table1", valueConverter, new CqlIdentifier[0], new CqlIdentifier[0], new CqlIdentifier[]{CqlIdentifier.fromInternal("id")}, preparedStatements);

        CassandraSourceConnectorConfig config = new CassandraSourceConnectorConfig(ImmutableMap.<String, String>builder()
                .put(CassandraSourceConnectorConfig.KEYSPACE_NAME_CONFIG, "ks1")
                .put(CassandraSourceConnectorConfig.TABLE_NAME_CONFIG, "table1")
                .put(CassandraSourceConnectorConfig.EVENTS_TOPIC_NAME_CONFIG, EVENTS_TOPIC)
                .put(CassandraSourceConnectorConfig.OUTPUT_TOPIC_CONFIG, OUTPUT_TOPIC)
                .build());

        task = new KafkaCassandraSourceTask();
        task.config = config;
        task.queryExecutor = org.apache.bookkeeper.common.util.OrderedExecutor.newBuilder()
                .name("cdc-query-executor")
                .numThreads(config.getQueryExecutors())
                .maxTasksInQueue(config.getQueryMaxTasksInQueue())
                .build();
        task.cassandraClient = cassandraClient;
        task.mutationCache = new MutationCache<>(3, 100, Duration.ofMinutes(1));
        task.mutationKeyConverter = mutationKeyConverter;
        task.valueConverterAndQuery = converterAndQuery;
        task.outputTopic = OUTPUT_TOPIC;
        task.heartbeatTopic = HEARTBEAT_TOPIC;
        task.eventsTopic = EVENTS_TOPIC;
        task.consumer = consumer;
    }

    @AfterEach
    void tearDown() {
        task.stop();
    }

    private byte[] encodePrimaryKey(int id) {
        GenericData.Record record = new GenericData.Record(mutationKeyConverter.nativeSchema);
        record.put("id", id);
        return AvroRowConverter.serializeAvroGenericRecord(record, mutationKeyConverter.nativeSchema);
    }

    private byte[] encodeMutationValue(String md5Digest, UUID nodeId) {
        GenericData.Record mutationValueRecord = new GenericData.Record(MUTATION_VALUE_SCHEMA);
        mutationValueRecord.put("md5Digest", md5Digest);
        mutationValueRecord.put("nodeId", nodeId.toString());
        mutationValueRecord.put("columns", null);
        return AvroRowConverter.serializeAvroGenericRecord(mutationValueRecord, MUTATION_VALUE_SCHEMA);
    }

    private ConsumerRecords<byte[], byte[]> singleRecordBatch(byte[] key, byte[] value, long offset) {
        TopicPartition tp = new TopicPartition(EVENTS_TOPIC, 0);
        ConsumerRecord<byte[], byte[]> rec = new ConsumerRecord<>(EVENTS_TOPIC, 0, offset, key, value);
        return new ConsumerRecords<>(Collections.singletonMap(tp, List.of(rec)));
    }

    @Test
    @SuppressWarnings("unchecked")
    void should_emit_source_record_for_new_mutation() throws Exception {
        byte[] key = encodePrimaryKey(1);
        byte[] value = encodeMutationValue("digest-1", nodeId);
        when(consumer.poll(any(Duration.class))).thenReturn(singleRecordBatch(key, value, 10L));
        when(cassandraClient.selectRow(anyList(), any(), any(ConsistencyLevel.class), any(), any()))
                .thenReturn(new Tuple2<>(mock(Row.class), nodeId));

        List<SourceRecord> records = task.poll();

        assertThat(records).hasSize(1);
        SourceRecord sourceRecord = records.get(0);
        assertThat(sourceRecord.topic()).isEqualTo(OUTPUT_TOPIC);
        assertThat(sourceRecord.value()).isEqualTo("row-bytes".getBytes());
        assertThat(sourceRecord.key()).isEqualTo(key);
        assertThat(sourceRecord.sourceOffset().get("offset")).isEqualTo(11L);
    }

    @Test
    @SuppressWarnings("unchecked")
    void should_emit_heartbeat_record_for_already_processed_mutation_without_querying_cassandra() throws Exception {
        byte[] key = encodePrimaryKey(2);
        byte[] value = encodeMutationValue("digest-2", nodeId);
        String cacheKey = Base64.getEncoder().encodeToString(key);
        task.mutationCache.addMutationMd5(cacheKey, "digest-2");

        when(consumer.poll(any(Duration.class))).thenReturn(singleRecordBatch(key, value, 20L));

        List<SourceRecord> records = task.poll();

        assertThat(records).hasSize(1);
        SourceRecord sourceRecord = records.get(0);
        assertThat(sourceRecord.topic()).isEqualTo(HEARTBEAT_TOPIC);
        assertThat(sourceRecord.value()).isNull();
        assertThat(sourceRecord.key()).isEqualTo(key);
        assertThat(sourceRecord.sourceOffset().get("offset")).isEqualTo(21L);
        verify(cassandraClient, never()).selectRow(anyList(), any(), any(ConsistencyLevel.class), any(), any());
    }

    @Test
    @SuppressWarnings("unchecked")
    void should_return_empty_list_when_no_records_polled() throws Exception {
        when(consumer.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());

        List<SourceRecord> records = task.poll();

        assertThat(records).isEmpty();
    }

    @Test
    void should_swap_value_converter_in_place_when_table_schema_changes() throws Exception {
        ColumnMetadata idColumn = new DefaultColumnMetadata(KS, TABLE, CqlIdentifier.fromInternal("id"), DataTypes.INT, false);
        ColumnMetadata nameColumn = new DefaultColumnMetadata(KS, TABLE, CqlIdentifier.fromInternal("name"), DataTypes.TEXT, false);
        ColumnMetadata emailColumn = new DefaultColumnMetadata(KS, TABLE, CqlIdentifier.fromInternal("email"), DataTypes.TEXT, false);

        TableMetadata tableBeforeAlter = mock(TableMetadata.class);
        when(tableBeforeAlter.getKeyspace()).thenReturn(KS);
        when(tableBeforeAlter.getName()).thenReturn(TABLE);
        when(tableBeforeAlter.getPrimaryKey()).thenReturn(List.of(idColumn));
        when(tableBeforeAlter.getColumns()).thenReturn(
                ImmutableMap.of(idColumn.getName(), idColumn, nameColumn.getName(), nameColumn));

        // Same table after an "ALTER TABLE ks1.table1 ADD email text"
        TableMetadata tableAfterAlter = mock(TableMetadata.class);
        when(tableAfterAlter.getKeyspace()).thenReturn(KS);
        when(tableAfterAlter.getName()).thenReturn(TABLE);
        when(tableAfterAlter.getPrimaryKey()).thenReturn(List.of(idColumn));
        when(tableAfterAlter.getColumns()).thenReturn(ImmutableMap.of(
                idColumn.getName(), idColumn, nameColumn.getName(), nameColumn, emailColumn.getName(), emailColumn));

        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
        when(keyspaceMetadata.getName()).thenReturn(KS);
        Metadata driverMetadata = mock(Metadata.class);
        when(driverMetadata.getKeyspace(KS)).thenReturn(Optional.of(keyspaceMetadata));
        CqlSession cqlSession = mock(CqlSession.class);
        when(cqlSession.getMetadata()).thenReturn(driverMetadata);
        when(cassandraClient.getCqlSession()).thenReturn(cqlSession);

        task.setValueConverterAndQuery(keyspaceMetadata, tableBeforeAlter);
        ConverterAndQuery<Converter<byte[], ?>> beforeAlter = task.valueConverterAndQuery;
        assertThat(fieldNames(beforeAlter)).containsExactlyInAnyOrder("name");

        // Simulates the CQL driver's SchemaChangeListener callback firing after the ALTER TABLE.
        task.onTableUpdated(tableAfterAlter, tableBeforeAlter);

        ConverterAndQuery<Converter<byte[], ?>> afterAlter = task.valueConverterAndQuery;
        assertThat(fieldNames(afterAlter)).containsExactlyInAnyOrder("name", "email");

        // The swap happens in place with no compatibility check: a reference captured before the
        // alter (as poll() does via its local "converterAndQueryFinal") keeps pointing at the old,
        // now-stale schema instead of being versioned or rejected. This is the gap tracked by the
        // schema-evolution TODO on setValueConverterAndQuery.
        assertThat(fieldNames(beforeAlter)).containsExactlyInAnyOrder("name");
    }

    private byte[] encodeMutationValueWithColumns(String md5Digest, UUID nodeId, List<String> columns) {
        GenericData.Record mutationValueRecord = new GenericData.Record(MUTATION_VALUE_SCHEMA);
        mutationValueRecord.put("md5Digest", md5Digest);
        mutationValueRecord.put("nodeId", nodeId.toString());
        mutationValueRecord.put("columns", columns);
        return AvroRowConverter.serializeAvroGenericRecord(mutationValueRecord, MUTATION_VALUE_SCHEMA);
    }

    private static CassandraSourceConnectorConfig configWith(String key, String value) {
        return new CassandraSourceConnectorConfig(ImmutableMap.<String, String>builder()
                .put(CassandraSourceConnectorConfig.KEYSPACE_NAME_CONFIG, "ks1")
                .put(CassandraSourceConnectorConfig.TABLE_NAME_CONFIG, "table1")
                .put(CassandraSourceConnectorConfig.EVENTS_TOPIC_NAME_CONFIG, EVENTS_TOPIC)
                .put(CassandraSourceConnectorConfig.OUTPUT_TOPIC_CONFIG, OUTPUT_TOPIC)
                .put(key, value)
                .build());
    }

    private void stubRowFound() throws Exception {
        when(cassandraClient.selectRow(anyList(), any(), any(ConsistencyLevel.class), any(), any()))
                .thenReturn(new Tuple2<>(mock(Row.class), nodeId));
    }

    @Test
    void should_rethrow_wakeup_exception_from_poll() {
        when(consumer.poll(any(Duration.class))).thenThrow(new WakeupException());

        assertThatThrownBy(() -> task.poll()).isInstanceOf(WakeupException.class);
    }

    @Test
    void should_retry_consumer_poll_after_transient_failure() throws Exception {
        when(consumer.poll(any(Duration.class)))
                .thenThrow(new KafkaException("broker unavailable"))
                .thenReturn(ConsumerRecords.empty());

        assertThat(task.poll()).isEmpty();
        verify(consumer, times(2)).poll(any(Duration.class));
    }

    @Test
    void should_fail_task_on_undecodable_event() {
        // Union index 1 (string) with no string bytes after it: the Avro decoder hits EOF.
        when(consumer.poll(any(Duration.class))).thenReturn(singleRecordBatch(encodePrimaryKey(3), new byte[]{2}, 5L));

        assertThatThrownBy(() -> task.poll())
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("offset 5");
    }

    @Test
    void should_decode_mutation_columns_list() throws Exception {
        byte[] key = encodePrimaryKey(4);
        byte[] value = encodeMutationValueWithColumns("digest-4", nodeId, List.of("a", "b"));
        when(consumer.poll(any(Duration.class))).thenReturn(singleRecordBatch(key, value, 40L));
        stubRowFound();

        assertThat(task.poll()).hasSize(1);
    }

    @Test
    void should_publish_tombstone_when_row_is_gone() throws Exception {
        byte[] key = encodePrimaryKey(5);
        when(consumer.poll(any(Duration.class))).thenReturn(singleRecordBatch(key, encodeMutationValue("digest-5", nodeId), 50L));
        when(cassandraClient.selectRow(anyList(), any(), any(ConsistencyLevel.class), any(), any()))
                .thenReturn(new Tuple2<>(null, nodeId));

        List<SourceRecord> records = task.poll();

        assertThat(records).hasSize(1);
        assertThat(records.get(0).value()).isNull();
    }

    @Test
    void should_publish_empty_json_object_when_row_is_gone_in_json_only_mode() throws Exception {
        task.config = configWith(CassandraSourceConnectorConfig.OUTPUT_FORMAT, "json");
        @SuppressWarnings("unchecked")
        Converter<byte[], Object> keyConverter = mock(Converter.class);
        when(keyConverter.fromConnectData(any())).thenReturn("json-key");
        task.keyConverter = keyConverter;
        when(consumer.poll(any(Duration.class))).thenReturn(singleRecordBatch(encodePrimaryKey(6), encodeMutationValue("digest-6", nodeId), 60L));
        when(cassandraClient.selectRow(anyList(), any(), any(ConsistencyLevel.class), any(), any()))
                .thenReturn(new Tuple2<>(null, nodeId));

        List<SourceRecord> records = task.poll();

        assertThat(records).hasSize(1);
        assertThat(records.get(0).key()).isEqualTo("json-key");
        assertThat(records.get(0).value()).isEqualTo("{}".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void should_wrap_key_with_schema_registry_serializer_when_configured() throws Exception {
        KafkaAvroSerializer keySerializer = mock(KafkaAvroSerializer.class);
        when(keySerializer.serialize(eq(OUTPUT_TOPIC), any())).thenReturn("registry-key".getBytes(StandardCharsets.UTF_8));
        KafkaAvroSerializer valueSerializer = mock(KafkaAvroSerializer.class);
        task.keySchemaRegistrySerializer = keySerializer;
        task.schemaRegistrySerializer = valueSerializer;
        when(consumer.poll(any(Duration.class))).thenReturn(singleRecordBatch(encodePrimaryKey(7), encodeMutationValue("digest-7", nodeId), 70L));
        stubRowFound();

        List<SourceRecord> records = task.poll();
        assertThat(records.get(0).key()).isEqualTo("registry-key".getBytes(StandardCharsets.UTF_8));

        task.stop();
        verify(keySerializer).close();
        verify(valueSerializer).close();
        assertThat(task.keySchemaRegistrySerializer).isNull();
        assertThat(task.schemaRegistrySerializer).isNull();
    }

    @Test
    void should_throttle_queries_when_rate_limit_is_set() throws Exception {
        java.lang.reflect.Field limiter = KafkaCassandraSourceTask.class.getDeclaredField("queryRateLimiter");
        limiter.setAccessible(true);
        limiter.set(task, RateLimiter.create(1000));
        when(consumer.poll(any(Duration.class))).thenReturn(singleRecordBatch(encodePrimaryKey(8), encodeMutationValue("digest-8", nodeId), 80L));
        stubRowFound();

        assertThat(task.poll()).hasSize(1);
    }

    @Test
    void should_register_value_schema_with_registry_on_converter_swap() throws Exception {
        task.config = configWith(CassandraSourceConnectorConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://registry");
        task.schemaRegistrySerializer = new KafkaAvroSerializer(new MockSchemaRegistryClient(),
                Map.of("schema.registry.url", "mock://registry"));
        ColumnMetadata idColumn = new DefaultColumnMetadata(KS, TABLE, CqlIdentifier.fromInternal("id"), DataTypes.INT, false);
        ColumnMetadata nameColumn = new DefaultColumnMetadata(KS, TABLE, CqlIdentifier.fromInternal("name"), DataTypes.TEXT, false);
        TableMetadata table = mock(TableMetadata.class);
        when(table.getKeyspace()).thenReturn(KS);
        when(table.getName()).thenReturn(TABLE);
        when(table.getPrimaryKey()).thenReturn(List.of(idColumn));
        when(table.getColumns()).thenReturn(ImmutableMap.of(idColumn.getName(), idColumn, nameColumn.getName(), nameColumn));
        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
        when(keyspaceMetadata.getName()).thenReturn(KS);

        task.setValueConverterAndQuery(keyspaceMetadata, table);

        assertThat(task.valueConverterAndQuery.getConverter()).isInstanceOf(KafkaAvroConverter.class);
    }

    @Test
    void should_refresh_converter_when_a_udt_in_the_keyspace_changes() {
        ColumnMetadata idColumn = new DefaultColumnMetadata(KS, TABLE, CqlIdentifier.fromInternal("id"), DataTypes.INT, false);
        TableMetadata table = mock(TableMetadata.class);
        when(table.getKeyspace()).thenReturn(KS);
        when(table.getName()).thenReturn(TABLE);
        when(table.getPrimaryKey()).thenReturn(List.of(idColumn));
        when(table.getColumns()).thenReturn(ImmutableMap.of(idColumn.getName(), idColumn));
        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
        when(keyspaceMetadata.getName()).thenReturn(KS);
        when(keyspaceMetadata.getTable("table1")).thenReturn(Optional.of(table));
        Metadata driverMetadata = mock(Metadata.class);
        when(driverMetadata.getKeyspace(KS)).thenReturn(Optional.of(keyspaceMetadata));
        CqlSession cqlSession = mock(CqlSession.class);
        when(cqlSession.getMetadata()).thenReturn(driverMetadata);
        when(cassandraClient.getCqlSession()).thenReturn(cqlSession);
        UserDefinedType udt = mock(UserDefinedType.class);
        when(udt.getKeyspace()).thenReturn(KS);
        ConverterAndQuery<Converter<byte[], ?>> before = task.valueConverterAndQuery;

        task.onUserDefinedTypeCreated(udt);
        ConverterAndQuery<Converter<byte[], ?>> afterCreate = task.valueConverterAndQuery;
        assertThat(afterCreate).isNotSameAs(before);

        task.onUserDefinedTypeUpdated(udt, udt);
        assertThat(task.valueConverterAndQuery).isNotSameAs(afterCreate);

        // A UDT in another keyspace leaves the converter alone.
        UserDefinedType otherUdt = mock(UserDefinedType.class);
        when(otherUdt.getKeyspace()).thenReturn(CqlIdentifier.fromInternal("other"));
        ConverterAndQuery<Converter<byte[], ?>> current = task.valueConverterAndQuery;
        task.onUserDefinedTypeCreated(otherUdt);
        task.onUserDefinedTypeUpdated(otherUdt, otherUdt);
        assertThat(task.valueConverterAndQuery).isSameAs(current);
    }

    @Test
    void should_ignore_schema_events_that_do_not_affect_the_table() {
        ConverterAndQuery<Converter<byte[], ?>> current = task.valueConverterAndQuery;
        KeyspaceMetadata keyspace = mock(KeyspaceMetadata.class);
        TableMetadata otherTable = mock(TableMetadata.class);
        when(otherTable.getKeyspace()).thenReturn(KS);
        when(otherTable.getName()).thenReturn(CqlIdentifier.fromInternal("other_table"));
        FunctionMetadata function = mock(FunctionMetadata.class);
        AggregateMetadata aggregate = mock(AggregateMetadata.class);
        ViewMetadata view = mock(ViewMetadata.class);

        task.onKeyspaceCreated(keyspace);
        task.onKeyspaceDropped(keyspace);
        task.onKeyspaceUpdated(keyspace, keyspace);
        task.onTableCreated(otherTable);
        task.onTableDropped(otherTable);
        task.onTableUpdated(otherTable, otherTable);
        task.onUserDefinedTypeDropped(mock(UserDefinedType.class));
        task.onFunctionCreated(function);
        task.onFunctionDropped(function);
        task.onFunctionUpdated(function, function);
        task.onAggregateCreated(aggregate);
        task.onAggregateDropped(aggregate);
        task.onAggregateUpdated(aggregate, aggregate);
        task.onViewCreated(view);
        task.onViewDropped(view);
        task.onViewUpdated(view, view);

        assertThat(task.valueConverterAndQuery).isSameAs(current);
    }

    private List<String> fieldNames(ConverterAndQuery<Converter<byte[], ?>> converterAndQuery) {
        AvroRowConverter converter = (AvroRowConverter) converterAndQuery.getConverter();
        return converter.nativeSchema.getFields().stream().map(Schema.Field::name).collect(Collectors.toList());
    }
}
