package com.datastax.cassandra.cdc.consumer;

import com.datastax.cassandra.cdc.*;
import com.datastax.cassandra.cdc.quasar.ClientConfiguration;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.vavr.Tuple2;
import org.elasticsearch.common.collect.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.io.IOException;
import java.util.ArrayList;
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

    final ClientConfiguration quasarClientConfiguration;
    final QuasarConfiguration quasarConfiguration;
    final ElasticsearchService elasticsearchService;
    final CassandraService cassandraService;
    final MeterRegistry meterRegistry;

    public QuasarConsumer(ClientConfiguration quasarClientConfiguration,
                          QuasarConfiguration quasarConfiguration,
                          ElasticsearchService elasticsearchService,
                          CassandraService cassandraService,
                          MeterRegistry meterRegistry) {
        this.quasarClientConfiguration = quasarClientConfiguration;
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
                .thenComposeAsync(cacheValue -> {
                    logger.debug("Document id={} last synced cacheValue={}", pk.id(), cacheValue);
                    if(cacheValue == null) {
                        // Elasticsearch is not available, retry later
                        CompletableFuture<Long> completedFuture = new CompletableFuture<>();
                        completedFuture.completeExceptionally(nack(pk, mutationValue, new IllegalStateException("Elasticsearch not available")));
                        return completedFuture;
                    }
                    if(mutationValue.getWritetime() < cacheValue.absoluteWritetime() && cacheValue.isConsistent()) {
                        // update is obsolete, hidden by another doc or a tombstone
                        return CompletableFuture.completedFuture(ack(pk, mutationValue, cacheValue.absoluteWritetime()));
                    }

                    CompletionStage<Tuple2<String, ConsistencyLevel>> readFuture;
                    if(mutationValue.getDocument() != null) {
                        // the payload contains the document source (or "" if deleted)
                        readFuture = CompletableFuture.completedFuture(new Tuple2<>(mutationValue.getDocument(), ConsistencyLevel.LOCAL_ONE));
                    } else {
                        // Read from cassandra if needed
                        readFuture = cassandraService.selectRowAsync(pk, mutationValue.getNodeId(), new ArrayList<>(quasarConfiguration.consistencies));
                    }
                    return readFuture.thenCompose(tuple2 -> {
                        try {
                            long consistent = tuple2._2.equals(ConsistencyLevel.ALL) ? 1L : -1L;
                            if (tuple2._1 == null || tuple2._1.length() == 0) {
                                // delete the elasticsearch doc
                                return elasticsearchService.delete(pk, mutationValue.getWritetime() * consistent)
                                        .thenApply(wt -> ack(pk, mutationValue, mutationValue.getWritetime() * consistent));
                            } else {
                                // insert the elasticsearch doc
                                Map<String, Object> source = mapper.readValue(tuple2._1, new TypeReference<Map<String, Object>>() {});
                                return elasticsearchService.index(pk, mutationValue.getWritetime() * consistent, source)
                                        .thenApply(wt -> ack(pk, mutationValue, mutationValue.getWritetime() * consistent));
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
