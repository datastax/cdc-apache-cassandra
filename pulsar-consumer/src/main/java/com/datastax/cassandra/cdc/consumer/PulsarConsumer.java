package com.datastax.cassandra.cdc.consumer;

import com.datastax.cassandra.cdc.MetricConstants;
import com.datastax.cassandra.cdc.MutationKey;
import com.datastax.cassandra.cdc.MutationValue;
import com.datastax.cassandra.cdc.pulsar.CDCSchema;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micronaut.context.ApplicationContext;
import io.micronaut.runtime.Micronaut;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Pulsar consumer writing to Elasticsearch.
 */
@Singleton
public class PulsarConsumer {
    private static final Logger logger = LoggerFactory.getLogger(PulsarConsumer.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    final PulsarConfiguration pulsarConfiguration;
    final CassandraService cassandraService;
    final MeterRegistry meterRegistry;
    final MutationCache mutationCache;
    final SchemaConverter schemaConverter;
    final ObjectMapper objectMapper;
    public PulsarConsumer(PulsarConfiguration pulsarConfiguration,
                          CassandraService cassandraService,
                          MutationCache mutationCache,
                          MeterRegistry meterRegistry,
                          SchemaConverter schemaConverter) {
        this.pulsarConfiguration = pulsarConfiguration;
        this.cassandraService = cassandraService;
        this.mutationCache = mutationCache;
        this.meterRegistry = meterRegistry;
        this.schemaConverter = schemaConverter;
        this.objectMapper = new ObjectMapper();
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
                    final MutationKey mutationKey = kv.getKey();
                    final MutationValue mutationValue = kv.getValue();

                    logger.debug("Message from producer={} msgId={} key={} value={}\n",
                            msg.getProducerName(), msg.getMessageId(), kv.getKey(), kv.getValue());

                    final Consumer<KeyValue<MutationKey, MutationValue>> consumerFinal = consumer;
                    final Message<KeyValue<MutationKey, MutationValue>> msgFinal = msg;
                    final PulsarClient client2 = client;
                    final List<Tag> tags = ImmutableList.of(Tag.of("keyspace", mutationKey.getKeyspace()), Tag.of("table", mutationKey.getTable()));

                    if (mutationCache.isProcessed(mutationKey, mutationValue.getMd5Digest()) == false) {
                        cassandraService.selectRowAsync(mutationKey, kv.getValue().getNodeId(), Lists.newArrayList(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_ONE))
                                .thenAcceptAsync(tuple -> {
                                    // update the target topic
                                    try {
                                        final String msgKey;
                                        List<ColumnMetadata> pkColumns = tuple._3.getTable(mutationKey.getTable()).get().getPrimaryKey();
                                        if (pkColumns.size() > 1) {
                                            JSONArray ja = new JSONArray();
                                            int i = 0;
                                            for(ColumnMetadata cm : pkColumns)
                                                ja.put(mutationKey.getPkColumns()[i++]);
                                            msgKey = ja.toString();
                                        } else {
                                            msgKey = mutationKey.getPkColumns()[0].toString();
                                        }

                                        String jsonString = tuple._1 == null ? null : tuple._1.getString(0);
                                        logger.debug("key={} value={}", msgKey, jsonString);

                                        // convert the JSON row to an AVRO message
                                        /*
                                        org.apache.avro.Schema avroSchema = schemaConverter.buildAvroSchema(tuple._3, mutationKey.getTable());
                                        byte[] avroMessage = new JsonAvroConverter().convertToAvro(json.getBytes(), avroSchema);

                                        SchemaInfo schemaInfo = SchemaInfo.builder()
                                                .schema(avroSchema.toString().getBytes(StandardCharsets.UTF_8))
                                                .type(SchemaType.AVRO)
                                                .name(mutationKey.getKeyspace()+"."+mutationKey.getTable())
                                                .properties(new HashMap<>())
                                                .build();
                                        Schema<GenericRecord> schema = Schema.generic(schemaInfo);
                                        */

                                        /*
                                        SchemaInfo  valueSchemaInfo = schemaConverter
                                                .buildSchema(tuple._3, mutationKey.getTable())
                                                .build(SchemaType.JSON);
                                        SchemaDefinition<?> valueSchemaDefinition = SchemaDefinition.builder()
                                                .withJsonDef(valueSchemaInfo.getSchemaDefinition())
                                                .build();
                                        Schema<GenericRecord> valueSchema = Schema.JSON(valueSchemaDefinition);



                                        GenericRecord genericRecord = null;
                                        if (jsonString != null) {
                                            try {
                                                GenericJsonSchema genericJsonSchema = (GenericJsonSchema)GenericSchemaImpl.of(valueSchemaInfo);
                                                genericRecord = new GenericJsonRecord(genericJsonSchema, jsonString);
                                            } catch(Exception e) {
                                                logger.warn("unexpected error", e);
                                            }
                                        }
                                        */

                                        Schema<KeyValue<String, String>> schema = Schema.KeyValue(Schema.STRING, Schema.STRING, KeyValueEncodingType.SEPARATED);
                                        Producer<KeyValue<String, String>> producer = client2.newProducer(schema)
                                                .topic(pulsarConfiguration.getSinkTopic())
                                                .batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS)
                                                .sendTimeout(10, TimeUnit.SECONDS)
                                                .blockIfQueueFull(true)
                                                .create();
                                        producer.newMessage(schema)
                                                .value(new KeyValue<String, String>(msgKey, jsonString))
                                                .sendAsync()
                                                .thenAccept(acknowledgeConsumer(consumerFinal, msgFinal, tags))
                                                .get();
                                    } catch(Exception e) {
                                        logger.error("error", e);
                                        negativeAcknowledge(consumerFinal, msgFinal, tags);
                                    }
                                });
                    } else {
                        acknowledge(consumerFinal, msgFinal, tags);
                    }
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
            meterRegistry.counter(MetricConstants.METRICS_PREFIX + "acked", tags).increment();
        } catch(PulsarClientException e) {
            logger.error("acknowledge error", e);
            consumer.negativeAcknowledge(message);
            meterRegistry.counter(MetricConstants.METRICS_PREFIX + "nacked", tags).increment();
        }
    }

    void negativeAcknowledge(final Consumer<KeyValue<MutationKey, MutationValue>> consumer,
                             final Message<KeyValue<MutationKey, MutationValue>> message,
                             final Iterable<Tag> tags) {
        consumer.negativeAcknowledge(message);
        meterRegistry.counter(MetricConstants.METRICS_PREFIX + "message_nacked", tags).increment();
    }

    public static void main(String[] args) {
        try(ApplicationContext context = Micronaut.run(PulsarConsumer.class, args)) {
            context.getBean(PulsarConsumer.class).consume();
        }
    }
}
