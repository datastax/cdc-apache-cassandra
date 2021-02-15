package com.datastax.cassandra.cdc.consumer;

import com.datastax.cassandra.cdc.*;
import com.datastax.cassandra.cdc.quasar.ClientConfiguration;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Quasar consumer writing to Elasticsearch.
 */
@Singleton
public class QuasarConsumer {
    private static final Logger logger = LoggerFactory.getLogger(QuasarConsumer.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    final ClientConfiguration quasarConfiguration;
    final ElasticsearchService elasticsearchService;
    final CassandraService cassandraService;
    final MeterRegistry meterRegistry;

    public QuasarConsumer(ClientConfiguration quasarConfiguration,
                          ElasticsearchService elasticsearchService,
                          CassandraService cassandraService,
                          MeterRegistry meterRegistry) {
        this.quasarConfiguration = quasarConfiguration;
        this.elasticsearchService = elasticsearchService;
        this.cassandraService = cassandraService;
        this.meterRegistry = meterRegistry;
    }

    // Acknowledge the message so that it can be deleted by the message broker
    Long ack(final MutationKey pk,
             final MutationValue mutationValue,
             final long writetime) {
        meterRegistry.counter(MetricConstants.METRICS_PREFIX + "ack", pk.tags()).increment();
        logger.debug("doc pk={} synced writetime={}", pk, writetime);
        return writetime;
    }

    Throwable nack(final MutationKey pk,
                   final MutationValue mutationValue,
                   Throwable throwable) {
        meterRegistry.counter(MetricConstants.METRICS_PREFIX + "nack", pk.tags()).increment();
        logger.warn("doc pk={} not synced, exception:", pk, throwable.getMessage());
        return throwable;
    }

    /**
     * @param pk
     * @param mutationValue
     * @return The last replicated writetime
     */
    CompletableFuture<Long> consume(MutationKey pk, MutationValue mutationValue) {
        logger.debug("Processing mutation pk={} mutation={}", pk, mutationValue);
        return elasticsearchService.getWritetime(pk)
                .thenComposeAsync(writetime -> {
                    logger.debug("Document id={} last synced writetime={}", pk.id(), writetime);
                    if(writetime == null) {
                        // Elasticsearch is not available, retry later
                        CompletableFuture<Long> completedFuture = new CompletableFuture<>();
                        completedFuture.completeExceptionally(nack(pk, mutationValue, new IllegalStateException("Elasticsearch not available")));
                        return completedFuture;
                    }
                    if(mutationValue.getWritetime() < writetime || mutationValue.getWritetime() < -writetime) {
                        // update is obsolete, hidden by another doc or a tombstone
                        return CompletableFuture.completedFuture(ack(pk, mutationValue, writetime));
                    }

                    if(mutationValue.getOperation().equals(Operation.DELETE)) {
                        // delete the doc and create a tombstone
                        return elasticsearchService.delete(pk, mutationValue.getWritetime())
                                .thenApply(wt -> ack(pk, mutationValue, mutationValue.getWritetime()));
                    }

                    CompletionStage<String> readFuture;
                    if(mutationValue.getDocument() != null) {
                        // the payload contains the document source
                        readFuture = CompletableFuture.completedFuture(mutationValue.getDocument());
                    } else {
                        // Read from cassandra if needed
                        readFuture = cassandraService.selectRowAsync(pk, mutationValue.getNodeId());
                    }
                    return readFuture.thenCompose(json -> {
                        try {
                            if (json == null || json.length() == 0) {
                                // delete the elasticsearch doc
                                return elasticsearchService.delete(pk, mutationValue.getWritetime())
                                        .thenApply(wt -> ack(pk, mutationValue, mutationValue.getWritetime()));
                            } else {
                                // insert the elasticsearch doc
                                Map<String, Object> source = mapper.readValue(json, new TypeReference<Map<String, Object>>() {});
                                return elasticsearchService.index(pk, mutationValue.getWritetime(), source)
                                        .thenApply(wt -> ack(pk, mutationValue, mutationValue.getWritetime()));
                            }
                        } catch(IOException e) {
                            CompletableFuture<Long> completedFuture = new CompletableFuture<>();
                            completedFuture.completeExceptionally(nack(pk, mutationValue, e));
                            return completedFuture;
                        }
                    });
                });
    }
}
