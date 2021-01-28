package com.datastax.cassandra.cdc.consumer;

import com.datastax.cassandra.cdc.*;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micronaut.context.ApplicationContext;
import io.micronaut.runtime.Micronaut;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.common.schema.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Pulsar consumer writing to Elasticsearch.
 */
@Singleton
public class PulsarConsumer {
    private static final Logger logger = LoggerFactory.getLogger(PulsarConsumer.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    final PulsarConfiguration pulsarConfiguration;
    final ElasticsearchService elasticsearchService;
    final CassandraService cassandraService;
    final MeterRegistry meterRegistry;

    public PulsarConsumer(PulsarConfiguration pulsarConfiguration,
                          ElasticsearchService elasticsearchService,
                          CassandraService cassandraService,
                          MeterRegistry meterRegistry) {
        this.pulsarConfiguration = pulsarConfiguration;
        this.elasticsearchService = elasticsearchService;
        this.cassandraService = cassandraService;
        this.meterRegistry = meterRegistry;
    }

    void consume() {
        PulsarClient client = null;
        Consumer<KeyValue<MutationKey, MutationValue>> consumer = null;
        try {
            client = PulsarClient.builder()
                    .serviceUrl(pulsarConfiguration.getServiceUrl())
                    .build();

            consumer = client.newConsumer(CDCSchema.kvSchema)
                    .consumerName("CDC Consumer")
                    .topic(pulsarConfiguration.getTopic())
                    .autoUpdatePartitions(true)
                    .subscriptionName(pulsarConfiguration.getSubscription())
                    .subscriptionType(SubscriptionType.Key_Shared)
                    .subscriptionMode(SubscriptionMode.Durable)
                    .keySharedPolicy(KeySharedPolicy.autoSplitHashRange())
                    .subscribe();

            logger.debug("Starting consumer topic={} subscription={}",pulsarConfiguration.getTopic(), pulsarConfiguration.getSubscription());
            while(true) {
                Message<KeyValue<MutationKey, MutationValue>> msg = null;
                try {
                    // Wait for a message
                    msg = consumer.receive();
                    final KeyValue<MutationKey, MutationValue> kv = msg.getValue();
                    final MutationKey pk = kv.getKey();

                    logger.debug("Message from producer={} msgId={} key={} value={}\n",
                            msg.getProducerName(), msg.getMessageId(), kv.getKey(), kv.getValue());

                    final Consumer<KeyValue<MutationKey, MutationValue>> consumerFinal = consumer;
                    final Message<KeyValue<MutationKey, MutationValue>> msgFinal = msg;

                    elasticsearchService.getWritetime(pk)
                        .thenAcceptAsync(writetime -> {
                            try {
                                logger.debug("Document id={} last synced writetime={}", kv.getKey().id(), writetime);
                                if (writetime == null) {
                                    // Elasticsearch is not available, retry later
                                    negativeAcknowledge(consumerFinal, msgFinal, pk.tags());
                                } else {
                                    if (kv.getValue().getWritetime() < writetime || kv.getValue().getWritetime() < -writetime) {
                                        // update is obsolete, hidden by another doc or a tombstone
                                        acknowledge(consumerFinal, msgFinal, pk.tags());
                                    } else {
                                        if (kv.getValue().getOperation().equals(Operation.DELETE)) {
                                            // delete the doc and create a tombstone
                                            elasticsearchService.delete(pk, kv.getValue().getWritetime())
                                                    .thenAcceptAsync(acknowledgeConsumer(consumerFinal, msgFinal, pk.tags()));
                                        } else {
                                            CompletionStage<String> readFuture;
                                            if (kv.getValue().getDocument() != null) {
                                                // the payload contains the document source
                                                readFuture = CompletableFuture.completedFuture(kv.getValue().getDocument());
                                            } else {
                                                // Read from cassandra if needed
                                                readFuture = cassandraService.selectRowAsync(pk, kv.getValue().getNodeId());
                                            }
                                            readFuture.thenAcceptAsync(json -> {
                                                // update Elasticsearch
                                                try {
                                                    Map<String, Object> source = mapper.readValue(json, new TypeReference<Map<String, Object>>() {});
                                                    elasticsearchService.index(pk, kv.getValue().getWritetime(), source)
                                                            .thenAcceptAsync(acknowledgeConsumer(consumerFinal, msgFinal, pk.tags()));
                                                } catch(IOException e) {
                                                    logger.error("Elasticsearch indexing error", e);
                                                    negativeAcknowledge(consumerFinal, msgFinal, pk.tags());
                                                }
                                            });
                                        }
                                    }
                                }
                            } catch(Exception e) {
                                logger.error("error", e);
                                negativeAcknowledge(consumerFinal, msgFinal, pk.tags());
                            }
                        });
                } catch(Exception e) {
                    // Message failed to process, redeliver later
                    logger.warn("error:", e);
                    if(msg != null)
                        negativeAcknowledge(consumer, msg, ImmutableList.of());
                }
            }
        } catch(Exception e) {
            logger.error("error:", e);
        } finally {
            if(consumer != null) {
                try {
                    consumer.close();
                } catch(Exception e) {
                }
            }
            if(client != null) {
                try {
                    client.close();
                } catch(Exception e) {
                }
            }
        }
    }

    <T> java.util.function.Consumer<T> acknowledgeConsumer(final Consumer<KeyValue<MutationKey, MutationValue>> consumer,
                                                   final Message<KeyValue<MutationKey, MutationValue>> message,
                                                   final Iterable<Tag> tags) {
        return new java.util.function.Consumer<T>() {
            @Override
            public void accept(T t) {
                acknowledge(consumer, message, tags);
            }
        };
    }

    // Acknowledge the message so that it can be deleted by the message broker
    void acknowledge(final Consumer<KeyValue<MutationKey, MutationValue>> consumer,
                     final Message<KeyValue<MutationKey, MutationValue>> message,
                     final Iterable<Tag> tags) {
        try {
            consumer.acknowledge(message);
            meterRegistry.counter(Metrics.METRICS_PREFIX + "message_acked", tags).increment();
        } catch(PulsarClientException e) {
            logger.error("acknowledge error", e);
            consumer.negativeAcknowledge(message);
            meterRegistry.counter(Metrics.METRICS_PREFIX + "message_nacked", tags).increment();
        }
    }

    void negativeAcknowledge(final Consumer<KeyValue<MutationKey, MutationValue>> consumer,
                             final Message<KeyValue<MutationKey, MutationValue>> message,
                             final Iterable<Tag> tags) {
        consumer.negativeAcknowledge(message);
        meterRegistry.counter(Metrics.METRICS_PREFIX + "message_nacked", tags).increment();
    }

    public static void main(String[] args) {
        try(ApplicationContext context = Micronaut.run(PulsarConsumer.class, args)) {
            context.getBean(PulsarConsumer.class).consume();
        }
    }
}
