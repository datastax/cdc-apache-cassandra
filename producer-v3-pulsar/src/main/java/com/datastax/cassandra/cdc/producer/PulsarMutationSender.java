package com.datastax.cassandra.cdc.producer;

import com.datastax.cassandra.cdc.MutationValue;
import com.datastax.cassandra.cdc.producer.converters.JsonConverter;
import lombok.extern.slf4j.Slf4j;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class PulsarMutationSender implements MutationSender<CFMetaData> , AutoCloseable {

    PulsarClient client;
    Map<String, Converter<?,List<CellData>,Object[]>> converters = new ConcurrentHashMap<>();
    Map<String, Producer<?>> producers = new ConcurrentHashMap<>();

    @SuppressWarnings("rawtypes")
    public Converter<?, List<CellData>, Object[]> getConverter(final CFMetaData tm) {
        String key = tm.ksName+"." + tm.cfName;
        return converters.computeIfAbsent(key, k -> {
            ByteBuffer bb = tm.params.extensions.get("cdc.keyConverter");
            if (bb != null) {
                try {
                    String converterClazz = ByteBufferUtil.string(bb);
                    Class<Converter<?,List<CellData>,Object[]>> converterClass = FBUtilities.classForName(converterClazz, "cdc key converter");
                    return converterClass.getDeclaredConstructor(CFMetaData.class).newInstance(tm);
                } catch(Exception e) {
                    log.error("unexpected error", e);
                }
            }
            return new JsonConverter(tm);   // default converter when not provided
        });
    }

    static int topicCount = 0;
    @SuppressWarnings("rawtypes")
    public Producer getProducer(final CFMetaData tm) {
        String topicName = PropertyConfig.topicPrefix + tm.ksName + "." + tm.cfName;
        String producerName = "pulsar-producer-" + topicName + topicCount++;
        return producers.compute(topicName, (k, v) -> {
                if (v == null) {
                    try {
                        Converter<?, List<CellData>, Object[]> keyConverter = getConverter(tm);
                        Schema<?> keyValueSchema = Schema.KeyValue(keyConverter.getSchema(), JSONSchema.of(MutationValue.class), KeyValueEncodingType.SEPARATED);
                        Producer producer = client.newProducer(keyValueSchema)
                                .producerName(producerName)
                                .topic(topicName)
                                .sendTimeout(15, TimeUnit.SECONDS)
                                .maxPendingMessages(3)
                                .hashingScheme(HashingScheme.Murmur3_32Hash)
                                .batcherBuilder(BatcherBuilder.KEY_BASED)
                                .create();
                        log.info("producer name={} created", producerName);
                        return producer;
                    } catch (Exception e) {
                        log.error("unexpected error", e);
                    }
                }
                return v;
        });
    }

    @Override
    public void initialize() throws PulsarClientException {
        try {
            this.client = PulsarClient.builder()
                    .serviceUrl(PropertyConfig.pulsarServiceUrl)
                    .build();
            log.info("Pulsar client connected");
        } catch(Exception e) {
            log.warn("Cannot connect to Pulsar:", e);
            throw e;
        }
    }

    @Override
    @SuppressWarnings({"rawtypes","unchecked"})
    public CompletionStage<Void> sendMutationAsync(final Mutation<CFMetaData> mutation) throws PulsarClientException {
        if (this.client == null) {
            initialize();
        }
        Producer<?> producer = getProducer(mutation.getMetadata());
        Converter<?, List<CellData>, Object[]> converter = getConverter(mutation.getMetadata());
        TypedMessageBuilder messageBuilder = producer.newMessage();

        return messageBuilder
                .value(new KeyValue(converter.toConnectData(mutation.primaryKeyCells()), mutation.mutationValue()))
                .sendAsync();
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
