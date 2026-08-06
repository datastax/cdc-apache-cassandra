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
package com.datastax.oss.cdc.agent;

import com.datastax.oss.cdc.Constants;
import com.datastax.oss.cdc.MutationValue;
import com.datastax.oss.cdc.agent.MutationSenderAvroUtil.SchemaAndWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

@Slf4j
public abstract class AbstractKafkaMutationSender<T> implements MutationSender<T>, AutoCloseable {

    volatile Producer<byte[], byte[]> producer;
    final Map<String, SchemaAndWriter> pkSchemas = new ConcurrentHashMap<>();

    final AgentConfig config;
    final boolean useMurmur3Partitioner;

    /** Back-pressure semaphore: limits in-flight send futures to maxPendingMessages. */
    volatile Semaphore pendingSemaphore;

    public AbstractKafkaMutationSender(AgentConfig config, boolean useMurmur3Partitioner) {
        this.config = config;
        this.useMurmur3Partitioner = useMurmur3Partitioner;
    }

    public abstract Schema getNativeSchema(String cql3Type);
    public abstract Object cqlToAvro(T t, String columnName, Object value);
    public abstract boolean isSupported(AbstractMutation<T> mutation);
    public abstract void incSkippedMutations();
    public abstract UUID getHostId();

    public SchemaAndWriter getPkSchema(String key) {
        return pkSchemas.get(key);
    }

    @Override
    public void initialize(AgentConfig config) {
        Properties props = new Properties();

        // Required
        Object bootstrapServers = config.get("bootstrapServers");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers != null ? bootstrapServers.toString() : "localhost:9092");

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        // Linger / batching
        Object batchDelayMs = config.get("batchDelayInMs");
        if (batchDelayMs != null) {
            long linger = toLong(batchDelayMs);
            if (linger > 0) {
                props.put(ProducerConfig.LINGER_MS_CONFIG, linger);
            }
        }

        // Security protocol
        Object securityProtocol = config.get("securityProtocol");
        if (securityProtocol != null) {
            props.put("security.protocol", securityProtocol.toString());
        }

        // SSL
        putIfPresent(props, config, "ssl.keystore.location",  "sslKeystoreLocation");
        putIfPresent(props, config, "ssl.keystore.password",  "sslKeystorePassword");
        putIfPresent(props, config, "ssl.truststore.location","sslTruststoreLocation");
        putIfPresent(props, config, "ssl.truststore.password","sslTruststorePassword");

        // SASL
        putIfPresent(props, config, "sasl.mechanism",    "saslMechanism");
        putIfPresent(props, config, "sasl.jaas.config",  "saslJaasConfig");

        // Pass through any remaining properties from the config file directly.
        // config.get() returns unrecognised file keys stored under their original names.
        // Anything that looks like a Kafka producer property (contains ".") is forwarded.
        // This lets operators set arbitrary producer configs (compression.type, acks, etc.)
        // without requiring AgentConfig changes.
        for (String key : config.propertyKeys()) {
            if (key.contains(".") && !props.containsKey(key)) {
                props.put(key, config.get(key).toString());
            }
        }

        int maxPending = 1000;
        Object mp = config.get("maxPendingMessages");
        if (mp != null) {
            maxPending = (int) toLong(mp);
        }
        this.pendingSemaphore = new Semaphore(maxPending);

        if (useMurmur3Partitioner) {
            props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
                    Murmur3KafkaPartitioner.class.getName());
        }

        this.producer = new KafkaProducer<byte[], byte[]>(props);
        log.info("Kafka producer connected to {}", props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    }

    private static void putIfPresent(Properties props, AgentConfig config, String kafkaKey, String configKey) {
        Object v = config.get(configKey);
        if (v != null) {
            props.put(kafkaKey, v.toString());
        }
    }

    private static long toLong(Object v) {
        if (v instanceof Number) {
            return ((Number) v).longValue();
        }
        return Long.parseLong(v.toString());
    }

    public SchemaAndWriter getAvroKeySchema(final TableInfo tableInfo) {
        return MutationSenderAvroUtil.getAvroKeySchema(tableInfo, pkSchemas, this::getNativeSchema);
    }

    public GenericRecord buildAvroKey(Schema keySchema, AbstractMutation<T> mutation) {
        return MutationSenderAvroUtil.buildAvroKey(keySchema, mutation,
                (colName, value) -> cqlToAvro(mutation.getMetadata(), colName, value));
    }

    public String topicName(final TableInfo tm) {
        return config.topicPrefix + tm.key();
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public CompletableFuture<?> sendMutationAsync(final AbstractMutation<T> mutation) {
        if (!isSupported(mutation)) {
            incSkippedMutations();
            return CompletableFuture.completedFuture(null);
        }
        try {
            if (this.producer == null) {
                synchronized (this) {
                    if (this.producer == null) {
                        initialize(config);
                    }
                }
            }

            SchemaAndWriter schemaAndWriter = getAvroKeySchema(mutation);
            byte[] keyBytes = MutationSenderAvroUtil.serializeAvroGenericRecord(
                    buildAvroKey(schemaAndWriter.schema, mutation), schemaAndWriter.writer);
            byte[] valueBytes = serializeMutationValue(mutation.mutationValue());

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topicName(mutation), keyBytes, valueBytes);
            record.headers().add(new RecordHeader(Constants.SEGMENT_AND_POSITION,
                    (mutation.getSegment() + ":" + mutation.getPosition()).getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader(Constants.TOKEN,
                    mutation.getToken().toString().getBytes(StandardCharsets.UTF_8)));
            if (mutation.getTs() != -1) {
                record.headers().add(new RecordHeader(Constants.WRITETIME,
                        Long.toString(mutation.getTs()).getBytes(StandardCharsets.UTF_8)));
            }

            pendingSemaphore.acquire();
            CompletableFuture<Void> future = new CompletableFuture<>();
            producer.send(record, (metadata, ex) -> {
                pendingSemaphore.release();
                if (ex != null) {
                    future.completeExceptionally(ex);
                } else {
                    future.complete(null);
                }
            });
            return future;
        } catch (Exception e) {
            CompletableFuture failed = new CompletableFuture<>();
            failed.completeExceptionally(e);
            return failed;
        }
    }

    // Schema for MutationValue: {md5Digest: string, nodeId: uuid-string, columns: [string]}
    static final Schema MUTATION_VALUE_SCHEMA;
    private static final org.apache.avro.generic.GenericDatumWriter<org.apache.avro.generic.GenericRecord>
            MUTATION_VALUE_WRITER;

    static {
        Schema nullableString = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));
        Schema nullableStringArray = Schema.createUnion(Schema.create(Schema.Type.NULL),
                Schema.createArray(Schema.create(Schema.Type.STRING)));
        MUTATION_VALUE_SCHEMA = Schema.createRecord("MutationValue", null, "com.datastax.oss.cdc", false,
                java.util.Arrays.asList(
                        new Schema.Field("md5Digest", nullableString, null, (Object) null),
                        new Schema.Field("nodeId",    nullableString, null, (Object) null),
                        new Schema.Field("columns",   nullableStringArray, null, (Object) null)
                ));
        MUTATION_VALUE_WRITER = new org.apache.avro.generic.GenericDatumWriter<>(MUTATION_VALUE_SCHEMA);
    }

    byte[] serializeMutationValue(MutationValue mv) {
        try {
            org.apache.avro.generic.GenericData.Record record =
                    new org.apache.avro.generic.GenericData.Record(MUTATION_VALUE_SCHEMA);
            record.put("md5Digest", mv.getMd5Digest());
            record.put("nodeId",    mv.getNodeId() != null ? mv.getNodeId().toString() : null);
            record.put("columns",   mv.getColumns() != null
                    ? java.util.Arrays.asList(mv.getColumns()) : null);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = new EncoderFactory().binaryEncoder(out, null);
            MUTATION_VALUE_WRITER.write(record, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        if (producer != null) {
            synchronized (this) {
                if (producer != null) {
                    producer.close();
                    producer = null;
                }
            }
        }
    }
}
