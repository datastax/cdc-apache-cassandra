package com.datastax.cassandra.cdc.consumer;

import com.datastax.cassandra.cdc.CDCSchema;
import com.datastax.cassandra.cdc.EventKey;
import com.datastax.cassandra.cdc.EventValue;
import com.datastax.cassandra.cdc.PulsarConfiguration;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.context.ApplicationContext;
import io.micronaut.runtime.Micronaut;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.common.schema.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Map;

/**
 * Hello world!
 */
@Singleton
public class PulsarConsumer {
    private static final Logger logger = LoggerFactory.getLogger(PulsarConsumer.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    final PulsarConfiguration pulsarConfiguration;
    final ElasticsearchService elasticsearchService;
    final CassandraService cassandraService;

    public PulsarConsumer(PulsarConfiguration pulsarConfiguration,
                          ElasticsearchService elasticsearchService,
                          CassandraService cassandraService) {
        this.pulsarConfiguration = pulsarConfiguration;
        this.elasticsearchService = elasticsearchService;
        this.cassandraService = cassandraService;
    }

    void consume() {
        PulsarClient client = null;
        Consumer<KeyValue<EventKey, EventValue>> consumer = null;
        try {
            client = PulsarClient.builder()
                    .serviceUrl(pulsarConfiguration.getServiceUrl())
                    .build();

            consumer = client.newConsumer(CDCSchema.kvSchema)
                    .topic(pulsarConfiguration.getTopic())
                    .autoUpdatePartitions(true)
                    .subscriptionName(pulsarConfiguration.getSubscription())
                    .subscriptionType(SubscriptionType.Key_Shared)
                    //.readCompacted(true)
                    .subscribe();

            logger.debug("Starting consumer topic={} subscription={}",pulsarConfiguration.getTopic(), pulsarConfiguration.getSubscription());
            while(true) {
                Message<KeyValue<EventKey, EventValue>> msg = null;
                try {
                    // Wait for a message
                    msg = consumer.receive();
                    final KeyValue<EventKey, EventValue> kv = msg.getValue();
                    final EventKey pk = kv.getKey();

                    logger.debug("Message from producer={} msgId={} key={} value={}\n",
                            msg.getProducerName(), msg.getMessageId(), kv.getKey(), kv.getValue());

                    final Consumer<KeyValue<EventKey, EventValue>> consumerFinal = consumer;
                    final Message<KeyValue<EventKey, EventValue>> msgFinal = msg;

                    elasticsearchService.getWritetime(kv.getKey())
                        .thenAcceptAsync(writetime -> {
                            try {
                                logger.debug("Document id={} last synced writetime={}", kv.getKey().id(), writetime);
                                if (writetime == null) {
                                    // Elasticsearch is not available
                                    consumerFinal.negativeAcknowledge(msgFinal);
                                } else {
                                    if (kv.getValue().getWritetime() < writetime) {
                                        // update is obsolete
                                        consumerFinal.acknowledge(msgFinal);
                                    } else {
                                        // Read from cassandra
                                        cassandraService.selectRowAsync(pk, kv.getValue().getNodeId())
                                            .thenAcceptAsync(json -> {
                                                // update Elasticsearch
                                                try {
                                                    Map<String, Object> source = mapper.readValue(json, new TypeReference<Map<String, Object>>() {
                                                    });
                                                    elasticsearchService.index(pk, kv.getValue().getWritetime(), source)
                                                        .thenAcceptAsync(wt -> {
                                                            // Acknowledge the message so that it can be deleted by the message broker
                                                            try {
                                                                consumerFinal.acknowledge(msgFinal);
                                                            } catch(PulsarClientException e) {
                                                                logger.error("error", e);
                                                                consumerFinal.negativeAcknowledge(msgFinal);
                                                            }
                                                        });
                                                } catch(IOException e) {
                                                    logger.error("error", e);
                                                    consumerFinal.negativeAcknowledge(msgFinal);
                                                }
                                            });
                                    }
                                }
                            } catch(Exception e) {
                                logger.error("error", e);
                                consumerFinal.negativeAcknowledge(msgFinal);
                            }
                        });
                } catch(Exception e) {
                    // Message failed to process, redeliver later
                    if(msg != null)
                        consumer.negativeAcknowledge(msg);
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

    public static void main(String[] args) {
        try(ApplicationContext context = Micronaut.run(PulsarConsumer.class, args)) {
            context.getBean(PulsarConsumer.class).consume();
        }
    }
}
