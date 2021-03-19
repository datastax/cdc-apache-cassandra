package com.datastax.cassandra.cdc.producer;

import com.datastax.cassandra.cdc.MetricConstants;
import com.datastax.cassandra.cdc.MutationKey;
import com.datastax.cassandra.cdc.MutationValue;
import com.datastax.cassandra.cdc.converter.Converter;
import com.datastax.cassandra.cdc.producer.converters.JsonConverter;
import com.google.common.collect.ImmutableList;
import io.debezium.util.ObjectSizeCalculator;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;

import javax.inject.Singleton;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Singleton
@Slf4j
public class PulsarMutationSender implements MutationSender<KeyValue<MutationKey, MutationValue>> , AutoCloseable {

    final PulsarConfiguration pulsarConfiguration;
    final OffsetFileWriter offsetWriter;
    final MeterRegistry meterRegistry;

    PulsarClient client;
    Map<String, Converter<?,List<CellData>,Object[]>> converters = new HashMap<>();
    Map<String, Producer<?>> producers = new HashMap<>();

    AtomicReference<CommitLogPosition> sentOffset = new AtomicReference<>(new CommitLogPosition(0,0));

    PulsarMutationSender(PulsarConfiguration pulsarConfiguration,
                         OffsetFileWriter offsetFileWriter,
                         MeterRegistry meterRegistry) {
        this.pulsarConfiguration = pulsarConfiguration;
        this.offsetWriter = offsetFileWriter;
        this.meterRegistry = meterRegistry;
    }

    @SuppressWarnings("rawtypes")
    public Converter<?, List<CellData>, Object[]> getConverter(final TableMetadata tm) {
        String key = tm.keyspace+"." + tm.name;
        return converters.computeIfAbsent(key, k -> {
            ByteBuffer bb = tm.params.extensions.get("cdc.keyConverter");
            if (bb != null) {
                try {
                    String converterClazz = ByteBufferUtil.string(bb);
                    Class<Converter<?,List<CellData>,Object[]>> converterClass = FBUtilities.classForName(converterClazz, "cdc key converter");
                    return converterClass.getDeclaredConstructor(TableMetadata.class).newInstance(tm);
                } catch(Exception e) {
                    log.error("unexpected error", e);
                }
            }
            return new JsonConverter(tm);   // default converter when not provided
        });
    }

    @SuppressWarnings("rawtypes")
    public Producer getProducer(final TableMetadata tm) {
        String key = tm.keyspace + "." + tm.name;
        return producers.computeIfAbsent(key, k -> {
            try {
                Converter<?, List<CellData>, Object[]> keyConverter = getConverter(tm);
                Schema<?> keyValueSchema = Schema.KeyValue(keyConverter.getSchema(), JSONSchema.of(MutationValue.class), KeyValueEncodingType.SEPARATED);
                return client.newProducer(keyValueSchema)
                        .producerName("pulsar-producer-" + pulsarConfiguration.getName() + "-" + key)
                        .topic(pulsarConfiguration.getTopicPrefix() + "-" + key)
                        .sendTimeout(15, TimeUnit.SECONDS)
                        .maxPendingMessages(3)
                        .hashingScheme(HashingScheme.Murmur3_32Hash)
                        .batcherBuilder(BatcherBuilder.KEY_BASED)
                        .create();
            } catch(Exception e) {
                log.error("unexpected error", e);
            }
            return null;
        });
    }

    @Override
    public void initialize() throws Exception {
        if (pulsarConfiguration.getServiceUrl() != null) {
            this.client = PulsarClient.builder()
                    .serviceUrl(pulsarConfiguration.getServiceUrl())
                    .build();

        }
    }

    @Override
    public CommitLogPosition sentOffset() {
        return this.sentOffset.get();
    }

    @Override
    @SuppressWarnings({"rawtypes","unchecked"})
    public CompletionStage<Void> sendMutationAsync(final Mutation mutation) {
        Producer<?> producer = getProducer(mutation.getTableMetadata());
        Converter<?, List<CellData>, Object[]> converter = getConverter(mutation.getTableMetadata());
        TypedMessageBuilder messageBuilder = producer.newMessage();

        return messageBuilder
                .value(new KeyValue(converter.toConnectData(mutation.primaryKeyCells()), mutation.mutationValue()))
                .sendAsync()
                .thenAccept(msgId -> {
                    this.sentOffset.set(mutation.getSource().commitLogPosition);
                    List<Tag> tags = ImmutableList.of(Tag.of("keyspace", mutation.getTableMetadata().keyspace), Tag.of("table", mutation.getTableMetadata().name));
                    meterRegistry.counter(MetricConstants.METRICS_PREFIX + "sent", tags).increment();
                    meterRegistry.counter(MetricConstants.METRICS_PREFIX + "sent_in_bytes", tags).increment(ObjectSizeCalculator.getObjectSize(mutation));
                    offsetWriter.notCommittedEvents++;
                    offsetWriter.maybeCommitOffset(mutation);
                });
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
        try {
            this.client.close();
        } catch(PulsarClientException e) {
            e.printStackTrace();
        }
    }
}
