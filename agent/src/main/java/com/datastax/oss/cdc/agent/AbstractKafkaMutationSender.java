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
import com.google.common.util.concurrent.RateLimiter;
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
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Base class for Kafka-backed CDC mutation senders.
 *
 * <h3>Why Avro?</h3>
 * Kafka has no built-in schema system — both key and value are raw {@code byte[]}.
 * The PK key is not a single scalar; it can span multiple columns of mixed CQL types
 * (e.g. {@code id text, bucket int, ts timeuuid}).  Some binary encoding is therefore
 * required to pack multiple typed values into a single byte array.
 *
 * <p>Avro was chosen because it was already present in the classpath and already used
 * by the Pulsar path ({@link AbstractPulsarMutationSender}) for exactly the same
 * purpose — building a per-table primary-key schema via {@link MutationSenderAvroUtil}.
 * Reusing it kept the Kafka path consistent with Pulsar and avoided introducing a
 * second serialisation format (e.g. JSON or Protobuf) for the same data.
 *
 * <h3>Serialisation format</h3>
 * Both key and value use Avro's {@code BinaryEncoder}, which writes <em>data bytes
 * only</em> — no schema, no field names, no length prefix.  The bytes are compact
 * but completely opaque without the schema:
 * <pre>
 *   "hello"  →  [0x0a, 0x68, 0x65, 0x6c, 0x6c, 0x6f]
 *                ^^^^  ──────────────────────────────
 *                zigzag-encoded length (5 → 10 = 0x0a)   UTF-8 bytes
 * </pre>
 * Producer (agent) and consumer must independently hold the <em>identical</em>
 * schema object.  If they differ, the consumer silently mis-parses fields or throws
 * {@code ArrayIndexOutOfBoundsException} — Kafka itself does not validate schemas.
 *
 * <p>This differs from self-describing alternatives:
 * <ul>
 *   <li><b>Avro Object Container File</b> — embeds schema JSON in the file header.</li>
 *   <li><b>Confluent Schema Registry</b> — prepends a magic byte ({@code 0x00}) and a
 *       4-byte schema ID; the registry resolves ID → schema at runtime.</li>
 *   <li><b>Pulsar</b> — the broker stores and negotiates schemas transparently, so
 *       {@link AbstractPulsarMutationSender} never handles this manually.</li>
 * </ul>
 *
 * <h3>Key schema — and how consumers discover it</h3>
 * The record <b>key</b> is the Avro-encoded primary key of the mutated row.
 * The schema is built per-table by {@link #getAvroKeySchema} from the table's column
 * definitions and cached in {@link #pkSchemas}.  Partition-key columns use their
 * native Avro type (e.g. {@code STRING} for {@code text}); clustering-key columns
 * are wrapped in a {@code ["null", type]} union because they are optional.
 *
 * <p><b>Consumers must reconstruct this schema independently</b> — the key bytes
 * carry no schema information.  The intended approach (mirroring the existing Pulsar
 * connector, {@code CassandraSource}) is for the consumer to connect to Cassandra
 * and query the table's primary key columns from {@code system_schema.columns}, then
 * apply the same type-mapping rules as {@link MutationSenderAvroUtil#getAvroKeySchema}
 * to rebuild the identical Avro schema before decoding.  A consumer that hardcodes
 * the schema for a specific table will break silently if that table's primary key
 * definition ever changes.
 *
 * <p>Simple partition key — {@code CREATE TABLE ks1.tbl1 (id text PRIMARY KEY)}:
 * <pre>
 *   Avro schema fields: [{"name":"id", "type":"string"}]
 *   key bytes → {"id": "hello"}
 * </pre>
 *
 * <p>Composite key — {@code CREATE TABLE ks1.tbl2 (org text, id int, ts timeuuid,
 * PRIMARY KEY ((org, id), ts))} where {@code (org, id)} is the partition key and
 * {@code ts} is a clustering column:
 * <pre>
 *   Avro schema fields: [{"name":"org",  "type":"string"},   ← partition key, plain type
 *                        {"name":"id",   "type":"int"},       ← partition key, plain type
 *                        {"name":"ts",   "type":["null","string"]}]  ← clustering, nullable union
 *   key bytes → {"org": "datastax", "id": 42, "ts": "550e8400-e29b-41d4-a716-446655440000"}
 * </pre>
 *
 * <h3>Value schema</h3>
 * The record <b>value</b> is the Avro-encoded {@link MutationValue}, using the
 * static {@link #MUTATION_VALUE_SCHEMA}: {@code md5Digest} (nullable string),
 * {@code nodeId} (nullable string), {@code columns} (nullable string array).
 * Example:
 * <pre>
 *   value bytes → Avro record {"md5Digest": "a1b2...", "nodeId": "1111-...", "columns": null}
 * </pre>
 * {@code columns} is always {@code null} — the agent publishes a change pointer,
 * not a full row image.  See {@link AbstractMutation#mutationValue()}.
 *
 * <p>Unlike the key schema, the value schema is <b>safe to hardcode on the consumer
 * side</b>: it is table-independent (every topic produces the same {@link MutationValue}
 * shape), deliberately minimal, and has been stable since the project's inception.
 * Consumers only need to mirror {@link #MUTATION_VALUE_SCHEMA} — three nullable fields,
 * no table metadata required.
 *
 * <h3>Schema stability and evolution risk</h3>
 * This is a <em>dirty</em> CDC events topic: it carries only change pointers
 * (PK + digest + nodeId), not full row data.  Both schemas are therefore
 * intentionally minimal and stable — {@link MutationValue} has not changed since
 * the project's inception and is not expected to change; the key schema is fixed by
 * the table's own primary key definition.
 *
 * <p>If either schema ever changes, consumers compiled against the old schema
 * would silently break.  Options if evolution becomes necessary:
 * <ul>
 *   <li>Integrate a Confluent (or other) Schema Registry and switch to
 *       {@code KafkaAvroSerializer} — consumers resolve the schema by ID at runtime.</li>
 *   <li>Prepend a 1-byte version tag so consumers can branch on format version.</li>
 *   <li>For the value only: switch to JSON ({@code JsonEncoder}), which is
 *       self-describing at the cost of larger messages.</li>
 * </ul>
 *
 * @param <T> Cassandra table-metadata type (e.g. {@code TableMetadata} for C4,
 *            {@code CFMetaData} for C3)
 */

@Slf4j
public abstract class AbstractKafkaMutationSender<T> implements MutationSender<T>, AutoCloseable {

    volatile Producer<byte[], byte[]> producer;
    final Map<String, SchemaAndWriter> pkSchemas = new ConcurrentHashMap<>();

    final AgentConfig config;
    final boolean useMurmur3Partitioner;

    /** Back-pressure semaphore: limits in-flight send futures to maxPendingMessages. */
    volatile Semaphore pendingSemaphore;

    /**
     * Throughput throttle on the Kafka send path: caps {@code producer.send()} calls to
     * {@code kafkaMutationSendRateLimit} messages/sec. {@code null} (the default, when the
     * setting is unset/non-positive) means genuinely no throttling at all -- sends skip the
     * rate limiter entirely, not merely a very-high-rate one. Only set (non-null) when an
     * operator has actually configured a positive rate, in {@link #buildProducerProperties}.
     */
    volatile RateLimiter sendRateLimiter;

    private static final Duration ACQUIRE_POLL_TIMEOUT = Duration.ofMillis(100);
    private static final int LOG_SAMPLE_ATTEMPTS = 10;

    final AtomicLong sendRateLimiterRetries = new AtomicLong(0);
    final AtomicLong pendingSemaphoreRetries = new AtomicLong(0);
    public long getSendRateLimiterRetries() {
        return sendRateLimiterRetries.get();
    }
    public long getPendingSemaphoreRetries() {
        return pendingSemaphoreRetries.get();
    }

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
        Properties props = buildProducerProperties(config);
        this.producer = new KafkaProducer<>(props);
        log.info("Kafka producer connected to {}", props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    }

    /** Package-private for testing. Builds the producer Properties from AgentConfig. */
    Properties buildProducerProperties(AgentConfig config) {
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

        // Pass through all remaining properties from the config file directly.
        // config.get() returns unrecognised file keys stored under their original names.
        // This lets operators set arbitrary producer configs (acks, compression.type, etc.)
        // without requiring AgentConfig changes.
        for (String key : config.propertyKeys()) {
            if (!props.containsKey(key)) {
                props.put(key, config.get(key).toString());
            }
        }

        if (useMurmur3Partitioner) {
            props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
                    Murmur3KafkaPartitioner.class.getName());
        }

        int maxPending = 1000;
        Object mp = config.get("maxPendingMessages");
        if (mp != null) {
            maxPending = (int) toLong(mp);
        }
        this.pendingSemaphore = new Semaphore(maxPending);

        // kafkaMutationSendRateLimit is a registered Setting with a 0.0 default, so
        // config.get() should always return a value here, never null -- but toDouble()
        // itself is null-safe (returns 0.0), so this is covered either way, including
        // if a non-standard AgentConfig (e.g. a test double, or a future refactor)
        // doesn't honor that guarantee.
        //
        // 0, a negative value, NaN, or +/-Infinity are all treated as "not configured"
        // (sendRateLimiter stays/becomes null -- genuinely no throttling, not a very-high
        // rate). Negative/NaN already fail "rate > 0" naturally; Infinity would pass it
        // (Infinity > 0 is true) and must be excluded explicitly: a Guava RateLimiter
        // created at an infinite rate and later setRate() to something finite is left
        // with broken internal state and silently stops throttling altogether -- verified
        // empirically against Guava 33.5.0-jre. Double.parseDouble("Infinity") is valid
        // input an operator could actually supply.
        double rate = toDouble(config.get("kafkaMutationSendRateLimit"));
        if (rate > 0 && Double.isFinite(rate)) {
            this.sendRateLimiter = RateLimiter.create(rate);
            log.info("Kafka mutation send rate limited to {} messages/sec", rate);
        } else {
            this.sendRateLimiter = null;
        }

        return props;
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

    private static double toDouble(Object v) {
        if (v == null) {
            return 0.0;
        }
        if (v instanceof Number) {
            return ((Number) v).doubleValue();
        }
        return Double.parseDouble(v.toString());
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
            ensureInitialized();
            ProducerRecord<byte[], byte[]> record = buildProducerRecord(mutation);

            // Local snapshot of the volatile field: one read instead of re-reading it on
            // every loop iteration inside acquireRateLimiter(). (sendRateLimiter is only
            // ever written once, from buildProducerProperties via ensureInitialized()'s
            // double-checked locking above, so there's no actual race to guard against --
            // this is purely avoiding redundant volatile reads in the loop.)
            RateLimiter rateLimiter = sendRateLimiter;
            if (rateLimiter != null) {
                acquireRateLimiter(rateLimiter);
            }
            acquirePendingSemaphore();

            CompletableFuture<Void> future = new CompletableFuture<>();
            try {
                producer.send(record, (metadata, ex) -> {
                    pendingSemaphore.release();
                    if (ex != null) {
                        future.completeExceptionally(ex);
                    } else {
                        future.complete(null);
                    }
                });
            } catch (Exception sendException) {
                // producer.send() can throw synchronously (e.g. SerializationException,
                // or IllegalStateException if the producer is already closed) before the
                // callback above is ever registered. Without this, the permit acquired
                // just above would leak forever since only the callback releases it.
                pendingSemaphore.release();
                future.completeExceptionally(sendException);
            }
            return future;
        } catch (Exception e) {
            // Thread.sleep() and Semaphore.tryAcquire(timeout, unit) above can both throw
            // InterruptedException; restore the interrupt flag rather than swallowing it,
            // so callers relying on it (e.g. a thread pool's shutdown/interruption check)
            // still see that this thread was interrupted.
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            CompletableFuture failed = new CompletableFuture<>();
            failed.completeExceptionally(e);
            return failed;
        }
    }

    /** Lazily creates the Kafka producer (and everything buildProducerProperties sets up) on first use. */
    private void ensureInitialized() {
        if (this.producer == null) {
            synchronized (this) {
                if (this.producer == null) {
                    initialize(config);
                }
            }
        }
    }

    /** Serializes the mutation's Avro key/value and attaches the standard CDC headers. */
    private ProducerRecord<byte[], byte[]> buildProducerRecord(AbstractMutation<T> mutation) {
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
        return record;
    }

    /**
     * Polls {@code rateLimiter} until a permit is acquired, retrying forever (never drops a
     * mutation). The explicit {@link Thread#sleep} below is required: unlike Semaphore's
     * timed tryAcquire, {@link RateLimiter#tryAcquire(Duration)} does not itself block when
     * it fails -- it returns {@code false} immediately if the wait would exceed the given
     * duration, without sleeping at all (verified empirically against Guava 33.5.0-jre).
     * Without the sleep here, this would busy-spin at full CPU under sustained throttling
     * instead of polling efficiently.
     */
    private void acquireRateLimiter(RateLimiter rateLimiter) throws InterruptedException {
        int attempt = 0;
        while (!rateLimiter.tryAcquire(ACQUIRE_POLL_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
            sendRateLimiterRetries.incrementAndGet();
            if (++attempt % LOG_SAMPLE_ATTEMPTS == 1) {
                log.debug("attempt {} to acquire send rate limiter failed (waiting for quota)", attempt);
            }
            Thread.sleep(ACQUIRE_POLL_TIMEOUT.toMillis());
        }
    }

    /**
     * Polls {@link #pendingSemaphore} until a permit is acquired, retrying forever (never
     * drops a mutation). Semaphore's timed tryAcquire properly blocks for up to the timeout
     * on its own (standard java.util.concurrent/AQS behavior), so no extra sleep is needed.
     */
    private void acquirePendingSemaphore() throws InterruptedException {
        int attempt = 0;
        while (!pendingSemaphore.tryAcquire(ACQUIRE_POLL_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
            pendingSemaphoreRetries.incrementAndGet();
            if (++attempt % LOG_SAMPLE_ATTEMPTS == 1) {
                log.debug("attempt {} to acquire max pending semaphore failed", attempt);
            }
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
