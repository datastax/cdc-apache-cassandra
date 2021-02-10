package com.datastax.cassandra.cdc.producer;

import com.datastax.cassandra.cdc.*;
import com.datastax.cassandra.cdc.pulsar.CDCSchema;
import com.datastax.cassandra.cdc.pulsar.PulsarConfiguration;
import io.debezium.util.ObjectSizeCalculator;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.common.schema.KeyValue;

import javax.inject.Singleton;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Singleton
public class PulsarMutationSender implements MutationSender<KeyValue<MutationKey, MutationValue>> {

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
    public CompletionStage<Void> sendMutationAsync(final Mutation mutation, String jsonDocument) {
        TypedMessageBuilder<KeyValue<MutationKey, MutationValue>> messageBuilder = this.producer.newMessage();
        MutationKey eventKey = mutation.mutationKey();
        return messageBuilder.value(new KeyValue<>(eventKey, mutation.mutationValue(jsonDocument))).sendAsync()
                .thenAccept(msgId -> {
                    this.sentOffset.set(mutation.getSource().commitLogPosition);
                    List<Tag> tags = mutation.getSource().getKeyspaceTable().tags();
                    meterRegistry.counter(MetricConstants.METRICS_PREFIX + "sent", tags).increment();
                    meterRegistry.counter(MetricConstants.METRICS_PREFIX + "sent_in_bytes", tags).increment(ObjectSizeCalculator.getObjectSize(mutation));
                    offsetWriter.notCommittedEvents++;
                    offsetWriter.maybeCommitOffset(mutation);
                });
    }
}
