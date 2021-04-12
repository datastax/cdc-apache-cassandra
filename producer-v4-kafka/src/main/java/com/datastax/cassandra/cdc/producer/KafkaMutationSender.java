package com.datastax.cassandra.cdc.producer;

import com.datastax.cassandra.cdc.MutationValue;
import com.google.common.collect.ImmutableMap;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.debezium.data.Uuid;
import io.debezium.time.Date;
import io.debezium.time.NanoDuration;
import io.debezium.time.Time;
import io.debezium.time.Timestamp;
import lombok.extern.slf4j.Slf4j;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.storage.Converter;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

/**
 * Send a Kafka Connect message to the "dirty" topic where
 * key is serialized with the specified KeyConverter (the JsonConverter by default),
 * and the MutationValue serialized with the AvroConverter as a Struct.
 */
@Slf4j
public class KafkaMutationSender implements MutationSender<TableMetadata> , AutoCloseable {

    final Map<String, Converter> converters = new ConcurrentHashMap<>();
    final Map<String, Schema> keySchemas = new ConcurrentHashMap<>();

    final Schema valueSchema;
    final Converter valueConverter;

    KafkaProducer<?, ?> kafkaProducer;

    final Executor executor = Executors.newFixedThreadPool(1);

    KafkaMutationSender() {
        // Map Cassandra native types to Kafka schemas
        keySchemas.put(UTF8Type.instance.asCQL3Type().toString(), SchemaBuilder.string().optional().build());
        keySchemas.put(AsciiType.instance.asCQL3Type().toString(), SchemaBuilder.string().optional().build());
        keySchemas.put(BooleanType.instance.asCQL3Type().toString(), SchemaBuilder.bool().optional().build());
        keySchemas.put(BytesType.instance.asCQL3Type().toString(), SchemaBuilder.bytes().optional().build());
        keySchemas.put(ByteType.instance.asCQL3Type().toString(), SchemaBuilder.int8().optional().build());
        keySchemas.put(ShortType.instance.asCQL3Type().toString(), SchemaBuilder.int16().optional().build());
        keySchemas.put(Int32Type.instance.asCQL3Type().toString(), SchemaBuilder.int32().optional().build());
        keySchemas.put(IntegerType.instance.asCQL3Type().toString(), SchemaBuilder.int64().optional().build());
        keySchemas.put(LongType.instance.asCQL3Type().toString(), SchemaBuilder.int64().optional().build());

        keySchemas.put(FloatType.instance.asCQL3Type().toString(), SchemaBuilder.float32().optional().build());
        keySchemas.put(DoubleType.instance.asCQL3Type().toString(), SchemaBuilder.float64().optional().build());

        keySchemas.put(InetAddressType.instance.asCQL3Type().toString(), SchemaBuilder.int64().optional().build());

        keySchemas.put(TimestampType.instance.asCQL3Type().toString(), Timestamp.builder().optional());
        keySchemas.put(SimpleDateType.instance.asCQL3Type().toString(), Date.builder().optional());
        keySchemas.put(TimeType.instance.asCQL3Type().toString(), Time.builder().optional());
        keySchemas.put(DurationType.instance.asCQL3Type().toString(), NanoDuration.builder().optional());

        keySchemas.put(UUIDType.instance.asCQL3Type().toString(), Uuid.builder().optional());
        keySchemas.put(TimeUUIDType.instance.asCQL3Type().toString(), Uuid.builder().optional());

        // Kafka Converter for the MutationValue
        valueConverter = new AvroConverter();
        valueConverter.configure(ImmutableMap.of(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, PropertyConfig.kafkaRegistryUrl
        ), false);

        // Kafka Schema for MutationValue
        valueSchema = SchemaBuilder.struct()
                .name("com.datastax.cassandra.cdc.MutationValue")
                .doc("Dirty mutation info")
                .field("md5Digest", Schema.STRING_SCHEMA)
                .field("nodeId", Schema.STRING_SCHEMA)
                .field("columns", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
                .build();
        log.debug("Avro mutation valueSchema={}", valueSchema);
    }

    @SuppressWarnings("rawtypes")
    public Schema getKeySchema(final TableMetadata tm) {
        String key = tm.keyspace+"." + tm.name;
        return keySchemas.computeIfAbsent(key, k -> {
            Schema schema;
            List<ColumnMetadata> primaryKeyColumns = new ArrayList<>();
            tm.primaryKeyColumns().forEach(primaryKeyColumns::add);
            if (primaryKeyColumns.size() == 1) {
                schema = keySchemas.get(primaryKeyColumns.get(0).type.asCQL3Type().toString());
            } else {
                SchemaBuilder schemaBuilder = SchemaBuilder.struct()
                        .name(key)
                        .version(1)
                        .doc("Primary key schema for table "+key);
                for(ColumnMetadata cm : primaryKeyColumns) {
                    schemaBuilder.field(cm.name.toString(), keySchemas.get(primaryKeyColumns.get(0).type.asCQL3Type().toString()));
                }
                schema = schemaBuilder.build();
            }
            return schema;
        });
    }

    public Converter getConverter(final TableMetadata tm) {
        String key = tm.keyspace+"." + tm.name;
        return converters.computeIfAbsent(key, k -> {
            Converter converter = null;
            ByteBuffer bb = tm.params.extensions.get("cdc.keyConverter");
            if (bb != null) {
                try {
                    String converterClazz = ByteBufferUtil.string(bb);
                    Class<Converter> converterClass = FBUtilities.classForName(converterClazz, "cdc key converter");
                    converter = converterClass.getDeclaredConstructor().newInstance();
                    // TODO: configure converter
                } catch(Exception e) {
                    log.error("unexpected error", e);
                }
            } else {
                converter = new AvroConverter();   // default key converter
            }
            if (PropertyConfig.kafkaRegistryUrl != null) {
                converter.configure(ImmutableMap.of(
                        JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, true,
                        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, PropertyConfig.kafkaRegistryUrl
                        ), true);
            }
            return converter;
        });
    }

    @Override
    public void initialize() throws Exception {
        String producerName = "kafka-producer-" + StorageService.instance.getLocalHostId();
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, PropertyConfig.kafkaBrokers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, producerName);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArraySerializer.class.getName());
        //props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());
        this.kafkaProducer = new KafkaProducer<>(props);
        log.info("Kafka producer name={} created", producerName);
    }

    Object buildKey(Schema keySchema, List<CellData> primaryKey) {
        if (primaryKey.size() == 1) {
            return primaryKey.get(0).value;
        } else {
            Struct struct = new Struct(keySchema);
            for (CellData cellData : primaryKey) {
                struct.put(cellData.name, cellData.value);
            }
            return struct;
        }
    }

    Struct buildValue(MutationValue mutationValue) {
        log.debug("mutationValue={}", mutationValue);
        Struct struct = new Struct(this.valueSchema)
                .put("md5Digest", mutationValue.getMd5Digest())
                .put("nodeId", mutationValue.getNodeId().toString());
        if (mutationValue.getColumns() != null)
            struct.put("columns", Arrays.asList(mutationValue.getColumns()));
        return struct;
    }

    @Override
    @SuppressWarnings({"rawtypes","unchecked"})
    public CompletionStage<Void> sendMutationAsync(final Mutation<TableMetadata> mutation) throws Exception {
        Converter keyConverter = getConverter(mutation.getMetadata());
        Schema keySchema = getKeySchema(mutation.getMetadata());
        String topicName = PropertyConfig.topicPrefix + mutation.getMetadata().keyspace + "." + mutation.getMetadata().name;
        byte[] serializedKey = keyConverter.fromConnectData(topicName, keySchema, buildKey(keySchema, mutation.primaryKeyCells()));
        byte[] serializedValue = valueConverter.fromConnectData(topicName, valueSchema, buildValue(mutation.mutationValue()));
        ProducerRecord record = new ProducerRecord(topicName, serializedKey, serializedValue);
        log.debug("Sending kafka record={}", record);

        if (kafkaProducer == null) {
            initialize(); // lazy init
        }
        return CompletableFuture.runAsync(() -> {
            try {
                RecordMetadata metadata = (RecordMetadata) kafkaProducer.send(record).get();
                log.debug("Message sent metadata={}", metadata);
            } catch (InterruptedException | ExecutionException e) {
                throw new IllegalStateException(e);
            }
        }, executor);
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
    public void close() {
        if (this.kafkaProducer != null) {
            this.kafkaProducer.close();
        }
    }
}
