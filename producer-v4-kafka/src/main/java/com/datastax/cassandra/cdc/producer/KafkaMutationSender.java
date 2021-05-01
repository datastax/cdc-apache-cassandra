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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.storage.Converter;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.*;

/**
 * Send a Kafka Connect message to the "dirty" topic where
 * key is serialized with the specified KeyConverter (the JsonConverter by default),
 * and the MutationValue serialized with the AvroConverter as a Struct.
 */
@Slf4j
public class KafkaMutationSender implements MutationSender<TableMetadata> , AutoCloseable {

    public static final String SCHEMA_DOC_PREFIX = "Primary key schema for table ";

    final Map<String, Schema> schemas = new HashMap<>();

    final Schema valueSchema;
    final Converter valueConverter;
    final Converter keyConverter;

    KafkaProducer<?, ?> kafkaProducer; // lazy init

    final Executor executor = Executors.newFixedThreadPool(1);

    KafkaMutationSender() {
        // Map Cassandra native types to Kafka schemas
        schemas.put(UTF8Type.instance.asCQL3Type().toString(), SchemaBuilder.string().optional().build());
        schemas.put(AsciiType.instance.asCQL3Type().toString(), SchemaBuilder.string().optional().build());
        schemas.put(BooleanType.instance.asCQL3Type().toString(), SchemaBuilder.bool().optional().build());
        schemas.put(BytesType.instance.asCQL3Type().toString(), SchemaBuilder.bytes().optional().build());
        schemas.put(ByteType.instance.asCQL3Type().toString(), SchemaBuilder.int8().optional().build());
        schemas.put(ShortType.instance.asCQL3Type().toString(), SchemaBuilder.int16().optional().build());
        schemas.put(Int32Type.instance.asCQL3Type().toString(), SchemaBuilder.int32().optional().build());
        schemas.put(IntegerType.instance.asCQL3Type().toString(), SchemaBuilder.int64().optional().build());
        schemas.put(LongType.instance.asCQL3Type().toString(), SchemaBuilder.int64().optional().build());

        schemas.put(FloatType.instance.asCQL3Type().toString(), SchemaBuilder.float32().optional().build());
        schemas.put(DoubleType.instance.asCQL3Type().toString(), SchemaBuilder.float64().optional().build());

        schemas.put(InetAddressType.instance.asCQL3Type().toString(), SchemaBuilder.int64().optional().build());

        schemas.put(TimestampType.instance.asCQL3Type().toString(), Timestamp.builder().optional());
        schemas.put(SimpleDateType.instance.asCQL3Type().toString(), Date.builder().optional());
        schemas.put(TimeType.instance.asCQL3Type().toString(), Time.builder().optional());
        schemas.put(DurationType.instance.asCQL3Type().toString(), NanoDuration.builder().optional());

        schemas.put(UUIDType.instance.asCQL3Type().toString(), Uuid.builder().optional());
        schemas.put(TimeUUIDType.instance.asCQL3Type().toString(), Uuid.builder().optional());

        Map<String, String> converterConfig = ImmutableMap.of(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, ProducerConfig.kafkaSchemaRegistryUrl
        );

        // Kafka Converter for the Mutation key
        keyConverter = new AvroConverter();
        keyConverter.configure(converterConfig, true);

        // Kafka Converter for the MutationValue
        valueConverter = new AvroConverter();
        valueConverter.configure(converterConfig, false);

        // Kafka Schema for MutationValue
        valueSchema = SchemaBuilder.struct()
                .name("com.datastax.cassandra.cdc.MutationValue")
                .doc("Dirty cassandra mutation info")
                .field("md5Digest", Schema.STRING_SCHEMA)
                .field("nodeId", Schema.STRING_SCHEMA)
                .field("columns", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
                .build();
        log.debug("Avro mutation valueSchema={}", valueSchema);
    }

    @SuppressWarnings("rawtypes")
    public Schema getKeySchema(final TableMetadata tm) {
        String key = tm.keyspace + "." + tm.name;
        return schemas.computeIfAbsent(key, k -> {
            List<ColumnMetadata> primaryKeyColumns = new ArrayList<>();
            tm.primaryKeyColumns().forEach(primaryKeyColumns::add);
            if (primaryKeyColumns.size() == 1) {
                return schemas.get(primaryKeyColumns.get(0).type.asCQL3Type().toString());
            } else {
                SchemaBuilder schemaBuilder = SchemaBuilder.struct()
                        .name(key)
                        .version(1)
                        .doc(SCHEMA_DOC_PREFIX + k);
                int i = 0;
                for(ColumnMetadata cm : primaryKeyColumns) {
                    schemaBuilder.field(cm.name.toString(), schemas.get(primaryKeyColumns.get(i++).type.asCQL3Type().toString()));
                }
                return schemaBuilder.build();
            }
        });
    }

    @Override
    public void initialize() throws Exception {
        String producerName = "kafka-producer-" + StorageService.instance.getLocalHostId();
        Properties props = new Properties();
        props.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ProducerConfig.kafkaBrokers);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG, producerName);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArraySerializer.class.getName());
        props.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArraySerializer.class.getName());
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
        Schema keySchema = getKeySchema(mutation.getMetadata());
        String topicName = ProducerConfig.topicPrefix + mutation.getMetadata().keyspace + "." + mutation.getMetadata().name;
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
