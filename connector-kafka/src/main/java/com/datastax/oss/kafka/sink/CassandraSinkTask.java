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
package com.datastax.oss.kafka.sink;

import com.datastax.oss.cdc.CassandraClient;
import com.datastax.oss.cdc.CassandraSourceConnectorConfig;
import com.datastax.oss.cdc.CqlLogicalTypes;
import com.datastax.oss.cdc.MutationCache;
import com.datastax.oss.cdc.MutationValue;
import com.datastax.oss.cdc.MutationValueCodec;
import com.datastax.oss.cdc.Version;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListenerBase;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.pulsar.source.Converter;
import com.datastax.oss.pulsar.source.ConverterAndQuery;
import com.datastax.oss.pulsar.source.converters.NativeAvroConverter;
import com.datastax.oss.pulsar.source.converters.NativeJsonConverter;
import io.vavr.Tuple2;
import io.vavr.Tuple3;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Kafka Connect sink task: consumes CDC mutation events, queries Cassandra for the current row, and
 * publishes the row to the data topic. Mirrors the logic of the Pulsar {@code CassandraSource},
 * reusing the same converters ({@link NativeAvroConverter}/{@link NativeJsonConverter}, which emit
 * raw AVRO/JSON bytes), {@link CassandraClient}, {@link MutationCache} and configuration.
 *
 * <p>Serialization: registry-less raw AVRO (matching the agent default). The event key is the
 * AVRO-encoded primary key and the event value is a {@link MutationValue} (decoded via
 * {@link MutationValueCodec}). The data-topic key reuses the event key bytes; the value is the
 * AVRO/JSON encoding of the current row, or {@code null} (tombstone) for a delete.
 */
public class CassandraSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(CassandraSinkTask.class);

    static {
        // Register AVRO logical type conversions used by the primary-key / value encodings.
        SpecificData.get().addLogicalTypeConversion(new CqlLogicalTypes.CqlVarintConversion());
        SpecificData.get().addLogicalTypeConversion(new CqlLogicalTypes.CqlDecimalConversion());
        SpecificData.get().addLogicalTypeConversion(new NativeAvroConverter.CqlDurationConversion());
        SpecificData.get().addLogicalTypeConversion(new Conversions.UUIDConversion());
    }

    private CassandraSinkConfig config;
    private CassandraSourceConnectorConfig cassandraConfig;
    private CassandraClient cassandraClient;
    private MutationCache<String> mutationCache;
    private KafkaProducer<byte[], byte[]> producer;
    private String dataTopic;
    private Optional<Pattern> columnPattern = Optional.empty();

    private volatile NativeAvroConverter mutationKeyConverter;
    private volatile Schema pkAvroSchema;
    private volatile ConverterAndQuery valueConverterAndQuery;

    @SuppressWarnings("try") // SchemaChangeListener is AutoCloseable but its lifecycle is owned by the driver session
    private final SchemaChangeListener schemaChangeListener = new SchemaChangeListenerBase() {
        @Override
        public void onTableUpdated(TableMetadata current, TableMetadata previous) {
            if (current.getKeyspace().asInternal().equals(cassandraConfig.getKeyspaceName())
                    && current.getName().asInternal().equals(cassandraConfig.getTableName())) {
                KeyspaceMetadata ksm = cassandraClient.getCqlSession().getMetadata()
                        .getKeyspace(current.getKeyspace()).orElse(null);
                if (ksm != null) {
                    log.info("Schema change detected for {}.{}, rebuilding value converter",
                            cassandraConfig.getKeyspaceName(), cassandraConfig.getTableName());
                    setValueConverterAndQuery(ksm, current);
                }
            }
        }
    };

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = new CassandraSinkConfig(props);
        this.cassandraConfig = config.getCassandraConfig();
        this.dataTopic = config.getDataTopic();

        String columnsRegexp = cassandraConfig.getColumnsRegexp();
        if (columnsRegexp != null && !columnsRegexp.isEmpty() && !".*".equals(columnsRegexp)) {
            this.columnPattern = Optional.of(Pattern.compile(columnsRegexp));
        }

        this.mutationCache = new MutationCache<>(
                cassandraConfig.getCacheMaxDigests(),
                cassandraConfig.getCacheMaxCapacity(),
                Duration.ofMillis(cassandraConfig.getCacheExpireAfterMs()));

        if (config.getBootstrapServers() == null || config.getBootstrapServers().isEmpty()) {
            throw new IllegalArgumentException(CassandraSinkConfig.KAFKA_BOOTSTRAP_SERVERS + " is required");
        }
        this.producer = new KafkaProducer<>(buildProducerProperties());

        initCassandraClient();
        log.info("Started CassandraSinkTask for {}.{} -> data topic {}",
                cassandraConfig.getKeyspaceName(), cassandraConfig.getTableName(), dataTopic);
    }

    private Properties buildProducerProperties() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        return p;
    }

    private void initCassandraClient() {
        try {
            this.cassandraClient = new CassandraClient(cassandraConfig, Version.getVersion(),
                    "cassandra-kafka-sink-" + cassandraConfig.getKeyspaceName() + "." + cassandraConfig.getTableName(),
                    schemaChangeListener);
            Tuple2<KeyspaceMetadata, TableMetadata> tuple =
                    cassandraClient.getTableMetadata(cassandraConfig.getKeyspaceName(), cassandraConfig.getTableName());
            if (tuple._1 == null) {
                throw new IllegalArgumentException(String.format(Locale.ROOT,
                        "Keyspace %s does not exist", cassandraConfig.getKeyspaceName()));
            }
            if (tuple._2 == null) {
                throw new IllegalArgumentException(String.format(Locale.ROOT,
                        "Table %s.%s does not exist", cassandraConfig.getKeyspaceName(), cassandraConfig.getTableName()));
            }
            this.mutationKeyConverter = new NativeAvroConverter(tuple._1, tuple._2, tuple._2.getPrimaryKey());
            this.pkAvroSchema = mutationKeyConverter.nativeSchema;
            setValueConverterAndQuery(tuple._1, tuple._2);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize Cassandra client", e);
        }
    }

    private boolean isPrimaryKeyOnlyTable(TableMetadata tableMetadata) {
        return tableMetadata.getColumns().size() == tableMetadata.getPrimaryKey().size()
                && new HashSet<>(tableMetadata.getPrimaryKey()).containsAll(tableMetadata.getColumns().values());
    }

    @SuppressWarnings("rawtypes") // ConverterAndQuery holds a raw Converter (reused from the Pulsar connector)
    private synchronized void setValueConverterAndQuery(KeyspaceMetadata ksm, TableMetadata tableMetadata) {
        boolean pkOnly = isPrimaryKeyOnlyTable(tableMetadata);
        List<ColumnMetadata> columns = tableMetadata.getColumns().values().stream()
                .filter(c -> cassandraConfig.isJsonOnlyOutputFormat() || pkOnly || !tableMetadata.getPrimaryKey().contains(c))
                .filter(c -> !columnPattern.isPresent() || columnPattern.get().matcher(c.getName().asInternal()).matches())
                .collect(Collectors.toList());
        List<ColumnMetadata> staticColumns = tableMetadata.getColumns().values().stream()
                .filter(ColumnMetadata::isStatic)
                .filter(c -> !tableMetadata.getPrimaryKey().contains(c))
                .filter(c -> !columnPattern.isPresent() || columnPattern.get().matcher(c.getName().asInternal()).matches())
                .collect(Collectors.toList());
        Converter valueConverter = (cassandraConfig.isJsonOutputFormat() || cassandraConfig.isJsonOnlyOutputFormat())
                ? new NativeJsonConverter(ksm, tableMetadata, columns)
                : new NativeAvroConverter(ksm, tableMetadata, columns);
        this.valueConverterAndQuery = new ConverterAndQuery(
                tableMetadata.getKeyspace().asInternal(),
                tableMetadata.getName().asInternal(),
                valueConverter,
                cassandraClient.buildProjectionClause(columns),
                cassandraClient.buildProjectionClause(staticColumns),
                cassandraClient.buildPrimaryKeyClause(tableMetadata),
                new ConcurrentHashMap<>());
        log.info("Value converter ready for {}.{} replicated columns={}",
                ksm.getName(), tableMetadata.getName(),
                columns.stream().map(c -> c.getName().asInternal()).collect(Collectors.toList()));
    }

    private synchronized PreparedStatement getSelectStatement(ConverterAndQuery cq, int whereClauseLength) {
        return cq.getPreparedStatements().computeIfAbsent(whereClauseLength, k ->
                cassandraClient.prepareSelect(
                        cq.getKeyspaceName(),
                        cq.getTableName(),
                        cq.getProjectionClause(k),
                        cq.getPrimaryKeyClause(),
                        k));
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        for (SinkRecord record : records) {
            try {
                processRecord(record);
            } catch (Exception e) {
                throw new RuntimeException(String.format(Locale.ROOT,
                        "Failed to process record from %s-%d@%d", record.topic(),
                        record.kafkaPartition() == null ? -1 : record.kafkaPartition(), record.kafkaOffset()), e);
            }
        }
        // Ensure produced data is durable before Connect advances the consumer offsets.
        producer.flush();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void processRecord(SinkRecord record) throws Exception {
        byte[] keyBytes = toBytes(record.key());
        byte[] valueBytes = toBytes(record.value());
        if (keyBytes == null) {
            log.warn("Skipping record with null key from {}@{}", record.topic(), record.kafkaOffset());
            return;
        }
        MutationValue mutationValue = MutationValueCodec.deserialize(valueBytes);
        if (mutationValue == null) {
            log.warn("Skipping record with null/invalid MutationValue from {}@{}", record.topic(), record.kafkaOffset());
            return;
        }

        String dedupKey = Base64.getEncoder().encodeToString(keyBytes);
        if (mutationCache.isMutationProcessed(dedupKey, mutationValue.getMd5Digest())) {
            log.debug("Mutation key={} md5={} already processed, skipping", dedupKey, mutationValue.getMd5Digest());
            return;
        }

        GenericRecord pkRecord = decodeAvro(keyBytes, pkAvroSchema);
        List<Object> pk = mutationKeyConverter.fromConnectData(pkRecord);
        List<Object> nonNullPkValues = pk.stream().filter(Objects::nonNull).collect(Collectors.toList());

        ConverterAndQuery cq = this.valueConverterAndQuery;
        // Mutable list: CassandraClient pops consistency levels as it downgrades on retry.
        List<ConsistencyLevel> consistencyLevels =
                new ArrayList<>(Arrays.asList(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_ONE));
        Tuple3<Row, ConsistencyLevel, UUID> tuple = cassandraClient.selectRow(
                nonNullPkValues,
                mutationValue.getNodeId(),
                consistencyLevels,
                getSelectStatement(cq, nonNullPkValues.size()),
                mutationValue.getMd5Digest());

        byte[] valueOut = tuple._1 == null ? null : (byte[]) cq.getConverter().toConnectData(tuple._1);

        if (ConsistencyLevel.LOCAL_QUORUM.equals(tuple._2)
                && (!cassandraConfig.getCacheOnlyIfCoordinatorMatch()
                || (tuple._3 != null && tuple._3.equals(mutationValue.getNodeId())))) {
            mutationCache.addMutationMd5(dedupKey, mutationValue.getMd5Digest());
        }

        // key passthrough (raw AVRO primary key); null value => tombstone (delete).
        producer.send(new ProducerRecord<>(dataTopic, keyBytes, valueOut));
        log.debug("Published row to {} for pk={}", dataTopic, nonNullPkValues);
    }

    private GenericRecord decodeAvro(byte[] bytes, Schema schema) throws Exception {
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        return reader.read(null, decoder);
    }

    private static byte[] toBytes(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof byte[]) {
            return (byte[]) value;
        }
        if (value instanceof ByteBuffer) {
            ByteBuffer bb = ((ByteBuffer) value).duplicate();
            byte[] out = new byte[bb.remaining()];
            bb.get(out);
            return out;
        }
        throw new IllegalArgumentException(
                "Expected byte[] key/value (use ByteArrayConverter); got " + value.getClass().getName());
    }

    @Override
    public void flush(Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets) {
        if (producer != null) {
            producer.flush();
        }
    }

    @Override
    public void stop() {
        log.info("Stopping CassandraSinkTask");
        if (producer != null) {
            try {
                producer.close();
            } catch (Exception e) {
                log.warn("Error closing producer", e);
            }
            producer = null;
        }
        if (cassandraClient != null) {
            try {
                cassandraClient.close();
            } catch (Exception e) {
                log.warn("Error closing Cassandra client", e);
            }
            cassandraClient = null;
        }
    }
}
