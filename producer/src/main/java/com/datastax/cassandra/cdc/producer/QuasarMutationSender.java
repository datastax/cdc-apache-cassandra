package com.datastax.cassandra.cdc.producer;

import com.datastax.cassandra.cdc.MutationKey;
import com.datastax.cassandra.cdc.MutationValue;
import com.datastax.cassandra.cdc.quasar.ClientConfiguration;
import com.datastax.cassandra.cdc.quasar.HttpClientFactory;
import com.datastax.cassandra.cdc.quasar.State;
import io.micrometer.core.instrument.MeterRegistry;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.pulsar.common.schema.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

@Requires(configuration = "default")
@Replaces(PulsarMutationSender.class)
@Singleton
public class QuasarMutationSender implements MutationSender<KeyValue<MutationKey, MutationValue>> {
    private static final Logger logger = LoggerFactory.getLogger(QuasarMutationSender.class);

    final ClientConfiguration quasarConfiguration;
    final OffsetFileWriter offsetWriter;
    final MeterRegistry meterRegistry;
    final HttpClientFactory httpClientFactory;

    AtomicReference<CommitLogPosition> sentOffset = new AtomicReference<>(new CommitLogPosition(0,0));
    volatile State state = State.NO_STATE;

    QuasarMutationSender(ClientConfiguration quasarConfiguration,
                         OffsetFileWriter offsetFileWriter,
                         HttpClientFactory httpClientFactory,
                         MeterRegistry meterRegistry) {
        this.quasarConfiguration = quasarConfiguration;
        this.offsetWriter = offsetFileWriter;
        this.meterRegistry = meterRegistry;
        this.httpClientFactory = httpClientFactory;
    }

    @Override
    public void initialize() throws Exception {
        try {
            httpClientFactory.clientForOrdinal(0, false).state()
                    .map(s -> {
                        this.state = s;
                        logger.info("initial versionedSize={}", s);
                        return s;
                    })
                    .blockingGet();
        } catch(Exception e) {
            logger.error("error:", e);
        }
    }

    @Override
    public CommitLogPosition sentOffset() {
        return this.sentOffset.get();
    }

    @Override
    public CompletionStage<Void> sendMutationAsync(final Mutation mutation, String jsonDocument) {
        MutationKey key = mutation.mutationKey();
        MutationValue value = mutation.mutationValue(jsonDocument);
        int hash = key.hash();
        int ordinal = hash % state.getSize();
        CompletableFuture<Long> cf = new CompletableFuture<>();
        try {
            httpClientFactory.clientForOrdinal(ordinal, false)
                    .replicate(key.getKeyspace(),
                            key.getTable(),
                            key.id(),
                            value.getOperation(),
                            value.getWritetime(),
                            value.getNodeId(),
                            jsonDocument)
                    .subscribe(cf::complete, t -> {
                        logger.warn("error:", t);
                        if (t instanceof HttpClientResponseException) {
                            HttpClientResponseException e = (HttpClientResponseException)t;
                            logger.warn("error status={} reason={}", e.getStatus().getCode(), e.getStatus().getReason());
                            switch(e.getStatus().getCode()) {
                                case 503: // service unavailable
                                case 404: // hash not managed
                                    Optional<State> optional = e.getResponse().getBody(State.class);
                                    if (optional.isPresent()) {
                                        this.state = state;
                                        logger.debug("New state={}, retrying later", this.state);
                                    }
                                    break;
                            }
                        }
                    });
        } catch(Exception e) {
            cf.completeExceptionally(e);
        }
        return cf.thenApply(wt -> null);
    }
}
