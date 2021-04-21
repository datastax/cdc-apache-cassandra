package com.datastax.oss.kafka.source;

import com.datastax.cassandra.cdc.CassandraClient;
import com.datastax.cassandra.cdc.MutationCache;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.*;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.vavr.Tuple2;
import io.vavr.Tuple3;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.Converter;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.nio.channels.Selector;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class CassandraSourceTask extends SourceTask implements SchemaChangeListener {

    public static final String DEFAULT_CONSUMER_GROUP_ID_PREFIX = "connector-group-";

    CassandraSourceConnectorConfig config;

    String eventsTopic;
    String dataTopic;
    String keyspaceName;
    String tableName;
    List<String> pkColumns;

    CassandraClient cassandraClient;
    final List<ConsistencyLevel> consistencyLevels = Collections.unmodifiableList(
            Lists.newArrayList(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_ONE));
    MutationCache<Object> mutationCache;
    volatile CassandraConverter cassandraConverter;

    Converter mutationKeyConverter, mutationValueConverter;
    Converter keyConverter, valueConverter;

    // the consumer is not thread-safe
    Consumer<byte[], byte[]> consumer = null;

    public CassandraSourceTask() {
    }

    /**
     * Get the version of this task. Usually this should be the same as the corresponding Connector class's version.
     *
     * @return the version, formatted as a String
     */
    @Override
    public String version() {
        return Version.getVersion();
    }

    /**
     * Start the Task. This should handle any configuration parsing and one-time setup of the task.
     *
     * @param config initial configuration
     */
    @Override
    public void start(Map<String, String> config) {
        start(config, null);
    }

    @VisibleForTesting
    @SuppressWarnings("unchecked")
    public void start(Map<String, String> props, CassandraClient client) {
        this.config = new CassandraSourceConnectorConfig(props);

        this.dataTopic = config.getDataTopic();
        this.eventsTopic = config.getEventsTopic();
        this.keyspaceName = config.getKeyspaceName();
        this.tableName = config.getTableName();

        this.mutationCache = new MutationCache<>(3, 10, Duration.ofHours(1));
        this.cassandraClient = client;
        if (this.cassandraClient == null) {
            this.cassandraClient = createClient(config.getContactPoints(), config.getLocalDc().get());
        }

        Tuple2<KeyspaceMetadata, TableMetadata> tuple = this.cassandraClient.getTableMetadata(this.keyspaceName, this.tableName);
        if (tuple._2 == null) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Table %s.%s does not exist.", keyspaceName, tableName));
        }
        setCassandraConverter(tuple._1, tuple._2);
        pkColumns = tuple._2.getPrimaryKey().stream().map(c -> c.getName().asCql(true)).collect(Collectors.toList());

        // converter props
        String schemaRegistryUrl = config.getSchemaRegistryUrl();
        Map<String, Object> converterProps = new HashMap<>();
        if (schemaRegistryUrl != null) {
            converterProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
            converterProps.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, true);
        }

        // events converters
        this.mutationKeyConverter = new AvroConverter();
        this.mutationKeyConverter.configure(converterProps, true);
        this.mutationValueConverter = new AvroConverter();
        this.mutationValueConverter.configure(converterProps, false);

        // data converters
        if (config.getKeyConverterClass() != null) {
            try {
                Class<Converter> converterClass = (Class<Converter>) config.getKeyConverterClass();
                this.keyConverter = converterClass.getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                log.error("Cannot instanciate key converter", e);
            }
        }
        if (this.keyConverter == null) {
            // default key converter
            this.keyConverter = new AvroConverter();
        }
        this.keyConverter.configure(converterProps, true);

        if (config.getValueConverterClass() != null) {
            try {
                Class<Converter> converterClass = (Class<Converter>) config.getValueConverterClass();
                this.valueConverter = converterClass.getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                log.error("Cannot instanciate value converter", e);
            }
        }
        if (valueConverter == null) {
            // default value converter
            this.valueConverter = new AvroConverter();
        }
        this.valueConverter.configure(converterProps, false);

        // Kafka consumer
        String consumerGroupId = DEFAULT_CONSUMER_GROUP_ID_PREFIX + config.getInstanceName();
        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArrayDeserializer.class.getName());

        synchronized(this) {
            this.consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(Collections.singletonList(eventsTopic));
        }

        log.info("Starting source connector name={} eventsTopic={} consumerGroupId={}}",
                config.getInstanceName(), eventsTopic, consumerGroupId);
    }

    private CassandraClient createClient(List<String> contactPoints, String localDc) {
        if (contactPoints.size() <= 0) {
            throw new RuntimeException("Empty cassandra contact points");
        }
        CqlSessionBuilder cqlSessionBuilder = CqlSession.builder()
                .withLocalDatacenter(localDc)
                .withSchemaChangeListener(this);
        log.info("Cassandra contact points={}", contactPoints);
        for (String contactPoint : contactPoints) {
            String[] hostPort = contactPoint.split(":");
            int port = hostPort.length > 1 ? Integer.valueOf(hostPort[1]) : 9042;
            InetSocketAddress endpoint = new InetSocketAddress(hostPort[0], port);
            cqlSessionBuilder.addContactPoint(endpoint);
        }
        CqlSession cqlSession = cqlSessionBuilder.build();
        cqlSession.setSchemaMetadataEnabled(true);
        return new CassandraClient(cqlSession);
    }

    void setCassandraConverter(KeyspaceMetadata ksm, TableMetadata tableMetadata) {
        this.cassandraConverter = new CassandraConverter(ksm,
                tableMetadata,
                tableMetadata.getColumns().values());
    }

    /**
     * <p>
     * Poll this source task for new records. If no data is currently available, this method
     * should block but return control to the caller regularly (by returning {@code null}) in
     * order for the task to transition to the {@code PAUSED} state if requested to do so.
     * </p>
     * <p>
     * The task will be {@link #stop() stopped} on a separate thread, and when that happens
     * this method is expected to unblock, quickly finish up any remaining processing, and
     * return.
     * </p>
     *
     * @return a list of source records
     */
    @Override
    public List<SourceRecord> poll() {
        ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(Duration.ofMillis(1000));
        List<SourceRecord> sourceRecords = new ArrayList<>(consumerRecords.count());
        // TODO: set the kafka partition
        int partition = 0;
        for (ConsumerRecord<byte[], byte[]> consumerRecord : consumerRecords) {
            log.debug("Message from producer={} msgId={} key={} value={}\n",
                    consumerRecord.partition(), consumerRecord.offset(), consumerRecord.key(), consumerRecord.value());

            SchemaAndValue keySchemaAndValue = mutationKeyConverter.toConnectData(this.eventsTopic, consumerRecord.key());
            Schema mutationKeySchema = keySchemaAndValue.schema();
            Object mutationKey = keySchemaAndValue.value();

            SchemaAndValue mutationSchemaAndValue = mutationValueConverter.toConnectData(this.eventsTopic, consumerRecord.value());
            Struct mutationStruct = (Struct) mutationSchemaAndValue.value();
            String md5Digest = mutationStruct.getString("md5Digest");
            String nodeId = mutationStruct.getString("nodeId");
            if (mutationCache.isMutationProcessed(mutationKey, md5Digest) == false) {
                try {
                    // ensure the schema is the one used when building the struct.
                    final CassandraConverter cassandraConverterFinal = this.cassandraConverter;
                    Map<String, Object> pk = new HashMap<>();
                    if (cassandraConverterFinal.getPrimaryKeyColumns().size() > 1) {
                        Struct struct = (Struct) keySchemaAndValue.value();
                        for (ColumnMetadata column : cassandraConverterFinal.getPrimaryKeyColumns()) {
                            String colName = column.getName().asCql(true);
                            pk.put(colName, struct.get(colName));
                        }
                    } else {
                        String colName = cassandraConverterFinal.getPrimaryKeyColumns().get(0).getName().asCql(true);
                        pk.put(colName, keySchemaAndValue.value());
                    }
                    Tuple3<Row, ConsistencyLevel, KeyspaceMetadata> tuple = cassandraClient.selectRow(
                            keyspaceName,
                            tableName,
                            pk,
                            UUID.fromString(nodeId),
                            new ArrayList<ConsistencyLevel>(consistencyLevels));
                    Object value = null;
                    if (tuple._1 != null) {
                        value = cassandraConverterFinal.buildStruct(tuple._1);
                    }
                    log.debug("key={} value={}", mutationKey, value);
                    SourceRecord sourceRecord = new SourceRecord(
                            ImmutableMap.of(),
                            ImmutableMap.of(),
                            dataTopic,
                            partition,
                            mutationKeySchema,
                            mutationKey,
                            cassandraConverterFinal.getSchema(),
                            value);
                    sourceRecords.add(sourceRecord);
                    mutationCache.addMutationMd5(mutationKey, md5Digest);
                } catch (Exception e) {
                    log.error("error", e);
                }
            }
        }
        return sourceRecords;
    }


    /**
     * Signal this SourceTask to stop. In SourceTasks, this method only needs to signal to the task that it should stop
     * trying to poll for new data and interrupt any outstanding poll() requests. It is not required that the task has
     * fully stopped. Note that this method necessarily may be invoked from a different thread than {@link #poll()} and
     * {@link #commit()}.
     * <p>
     * For example, if a task uses a {@link Selector} to receive data over the network, this method
     * could set a flag that will force {@link #poll()} to exit immediately and invoke
     * {@link Selector#wakeup() wakeup()} to interrupt any ongoing requests.
     */
    @Override
    public void stop() {
        if (this.consumer != null) {
            synchronized(this) {
                if (this.consumer != null) {
                    this.consumer.close();
                    this.consumer = null;
                }
            }
        }
    }


    @Override
    public void onKeyspaceCreated(@NonNull KeyspaceMetadata keyspaceMetadata) {

    }

    @Override
    public void onKeyspaceDropped(@NonNull KeyspaceMetadata keyspaceMetadata) {

    }

    @Override
    public void onKeyspaceUpdated(@NonNull KeyspaceMetadata keyspaceMetadata, @NonNull KeyspaceMetadata keyspaceMetadata1) {

    }

    @Override
    public void onTableCreated(@NonNull TableMetadata tableMetadata) {

    }

    @Override
    public void onTableDropped(@NonNull TableMetadata tableMetadata) {

    }

    @SneakyThrows
    @Override
    public void onTableUpdated(@NonNull TableMetadata current, @NonNull TableMetadata previous) {
        if (current.getKeyspace().asCql(true).equals(keyspaceName)
                && current.getName().asCql(true).equals(tableName)) {
            KeyspaceMetadata ksm = cassandraClient.getCqlSession().getMetadata().getKeyspace(keyspaceName).get();
            setCassandraConverter(ksm, current);
        }
    }

    @SneakyThrows
    @Override
    public void onUserDefinedTypeCreated(@NonNull UserDefinedType type) {
        if (type.getKeyspace().asCql(true).equals(keyspaceName)) {
            KeyspaceMetadata ksm = cassandraClient.getCqlSession().getMetadata().getKeyspace(keyspaceName).get();
            setCassandraConverter(ksm, ksm.getTable(tableName).get());
        }
    }

    @Override
    public void onUserDefinedTypeDropped(@NonNull UserDefinedType userDefinedType) {

    }

    @Override
    public void onUserDefinedTypeUpdated(@NonNull UserDefinedType userDefinedType, @NonNull UserDefinedType userDefinedType1) {
        if (userDefinedType.getKeyspace().asCql(true).equals(keyspaceName)) {
            KeyspaceMetadata ksm = cassandraClient.getCqlSession().getMetadata().getKeyspace(keyspaceName).get();
            setCassandraConverter(ksm, ksm.getTable(tableName).get());
        }
    }

    @Override
    public void onFunctionCreated(@NonNull FunctionMetadata functionMetadata) {

    }

    @Override
    public void onFunctionDropped(@NonNull FunctionMetadata functionMetadata) {

    }

    @Override
    public void onFunctionUpdated(@NonNull FunctionMetadata functionMetadata, @NonNull FunctionMetadata functionMetadata1) {

    }

    @Override
    public void onAggregateCreated(@NonNull AggregateMetadata aggregateMetadata) {

    }

    @Override
    public void onAggregateDropped(@NonNull AggregateMetadata aggregateMetadata) {

    }

    @Override
    public void onAggregateUpdated(@NonNull AggregateMetadata aggregateMetadata, @NonNull AggregateMetadata aggregateMetadata1) {

    }

    @Override
    public void onViewCreated(@NonNull ViewMetadata viewMetadata) {

    }

    @Override
    public void onViewDropped(@NonNull ViewMetadata viewMetadata) {

    }

    @Override
    public void onViewUpdated(@NonNull ViewMetadata viewMetadata, @NonNull ViewMetadata viewMetadata1) {

    }

    /**
     * Closes this resource, relinquishing any underlying resources.
     * This method is invoked automatically on objects managed by the
     * {@code try}-with-resources statement.
     *
     * <p>While this interface method is declared to throw {@code
     * Exception}, implementers are <em>strongly</em> encouraged to
     * declare concrete implementations of the {@code close} method to
     * throw more specific exceptions, or to throw no exception at all
     * if the close operation cannot fail.
     *
     * <p> Cases where the close operation may fail require careful
     * attention by implementers. It is strongly advised to relinquish
     * the underlying resources and to internally <em>mark</em> the
     * resource as closed, prior to throwing the exception. The {@code
     * close} method is unlikely to be invoked more than once and so
     * this ensures that the resources are released in a timely manner.
     * Furthermore it reduces problems that could arise when the resource
     * wraps, or is wrapped, by another resource.
     *
     * <p><em>Implementers of this interface are also strongly advised
     * to not have the {@code close} method throw {@link
     * InterruptedException}.</em>
     * <p>
     * This exception interacts with a thread's interrupted status,
     * and runtime misbehavior is likely to occur if an {@code
     * InterruptedException} is {@linkplain Throwable#addSuppressed
     * suppressed}.
     * <p>
     * More generally, if it would cause problems for an
     * exception to be suppressed, the {@code AutoCloseable.close}
     * method should not throw it.
     *
     * <p>Note that unlike the {@link Closeable#close close}
     * method of {@link Closeable}, this {@code close} method
     * is <em>not</em> required to be idempotent.  In other words,
     * calling this {@code close} method more than once may have some
     * visible side effect, unlike {@code Closeable.close} which is
     * required to have no effect if called more than once.
     * <p>
     * However, implementers of this interface are strongly encouraged
     * to make their {@code close} methods idempotent.
     *
     * @throws Exception if this resource cannot be closed
     */
    @Override
    public void close() throws Exception {
        if (this.cassandraClient != null)
            this.cassandraClient.close();
    }
}
