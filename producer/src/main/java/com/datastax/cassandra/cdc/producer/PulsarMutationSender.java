package com.datastax.cassandra.cdc.producer;

import com.datastax.cassandra.cdc.MetricConstants;
import com.datastax.cassandra.cdc.MutationKey;
import com.datastax.cassandra.cdc.MutationValue;
import com.datastax.cassandra.cdc.pulsar.CDCSchema;
import com.google.common.collect.ImmutableList;
import io.debezium.util.ObjectSizeCalculator;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.common.schema.KeyValue;

import javax.inject.Singleton;
import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Singleton
public class PulsarMutationSender implements MutationSender<KeyValue<MutationKey, MutationValue>> , AutoCloseable {

    final PulsarConfiguration pulsarConfiguration;
    final OffsetFileWriter offsetWriter;
    final MeterRegistry meterRegistry;

    PulsarClient client;
    Producer<KeyValue<MutationKey, MutationValue>> producer;
    AtomicReference<CommitLogPosition> sentOffset = new AtomicReference<>(new CommitLogPosition(0,0));

    PulsarMutationSender(PulsarConfiguration pulsarConfiguration,
                         OffsetFileWriter offsetFileWriter,
                         MeterRegistry meterRegistry) {
        this.pulsarConfiguration = pulsarConfiguration;
        this.offsetWriter = offsetFileWriter;
        this.meterRegistry = meterRegistry;
    }

    @Override
    public void initialize() throws Exception {
        if (pulsarConfiguration.getServiceUrl() != null) {
            this.client = PulsarClient.builder()
                    .serviceUrl(pulsarConfiguration.getServiceUrl())
                    .build();
            this.producer = client.newProducer(CDCSchema.kvSchema)
                    .producerName("pulsar-producer-" + pulsarConfiguration.getName())
                    .topic(pulsarConfiguration.getTopic())
                    .sendTimeout(15, TimeUnit.SECONDS)
                    .maxPendingMessages(3)
                    .hashingScheme(HashingScheme.Murmur3_32Hash)
                    .batcherBuilder(BatcherBuilder.KEY_BASED)
                    .create();
        }
    }

    @Override
    public CommitLogPosition sentOffset() {
        return this.sentOffset.get();
    }

    @Override
    public CompletionStage<Void> sendMutationAsync(final Mutation mutation) {
        TypedMessageBuilder<KeyValue<MutationKey, MutationValue>> messageBuilder = this.producer.newMessage();
        MutationKey mutationKey = mutation.mutationKey();
        return messageBuilder.value(new KeyValue<>(mutationKey, mutation.mutationValue())).sendAsync()
                .thenAccept(msgId -> {
                    this.sentOffset.set(mutation.getSource().commitLogPosition);
                    List<Tag> tags = ImmutableList.of(Tag.of("keyspace", mutationKey.getKeyspace()), Tag.of("table", mutationKey.getTable()));
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
