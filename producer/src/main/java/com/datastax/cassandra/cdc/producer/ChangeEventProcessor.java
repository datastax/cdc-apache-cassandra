/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.datastax.cassandra.cdc.producer;


import com.datastax.cassandra.cdc.CDCSchema;
import com.datastax.cassandra.cdc.EventKey;
import com.datastax.cassandra.cdc.EventValue;
import com.datastax.cassandra.cdc.PulsarConfiguration;
import com.datastax.cassandra.cdc.producer.exceptions.CassandraConnectorTaskException;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.schema.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * A thread that constantly polls records from the queue and emit them to Kafka via the KafkaRecordEmitter.
 * The processor is also responsible for marking the offset to file and deleting the commit log files.
 */
@Singleton
public class ChangeEventProcessor extends AbstractProcessor implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ChangeEventProcessor.class);
    private static final String NAME = "Change Event Queue Processor";

    public static final String ARCHIVE_FOLDER = "archive";
    public static final String ERROR_FOLDER = "error";

    final ChangeEventQueue queue;
    final String commitLogRelocationDir;

    final PulsarClient client;
    final Producer<KeyValue<EventKey, EventValue>> producer;

    private final FileOffsetWriter offsetWriter;
    private final OffsetFlushPolicy offsetFlushPolicy;
    private volatile long timeOfLastFlush = System.currentTimeMillis();
    private volatile Long notCommittedEvents = 0L;
    private final MeterRegistry meterRegistry;

    ChangeEventProcessor(CassandraConnectorConfiguration config,
                         PulsarConfiguration pulsarConfiguration,
                         ChangeEventQueue changeEventQueue,
                         FileOffsetWriter offsetWriter,
                         MeterRegistry meterRegistry) throws PulsarClientException {
        super(NAME, 0);
        this.queue = changeEventQueue;
        this.offsetWriter = offsetWriter;
        this.offsetFlushPolicy = new OffsetFlushPolicy.AlwaysFlushOffsetPolicy();
        this.commitLogRelocationDir = config.commitLogRelocationDir;
        this.client = PulsarClient.builder()
                .serviceUrl(pulsarConfiguration.getServiceUrl())
                .build();
        this.producer = client.newProducer(CDCSchema.kvSchema)
                .producerName("producer-1")
                .topic(pulsarConfiguration.getTopic())
                .sendTimeout(0, TimeUnit.SECONDS)
                .create();

        this.meterRegistry = meterRegistry;
        this.meterRegistry.gauge("notCommittedEvents", notCommittedEvents);

    }

    @Override
    public void process() throws InterruptedException {
        Event e, notSuccessfullProcessedEvent = null;
        while (true) {
            e = notSuccessfullProcessedEvent != null ? notSuccessfullProcessedEvent : queue.take(); // blocking
            try {
                processEvent(e);
                notSuccessfullProcessedEvent = null;
            } catch(Exception ex) {
                notSuccessfullProcessedEvent = e;
                logger.warn("emit error event={}", e);
                // TODO: exponential retry
                Thread.sleep(1000);
            }
            if (notSuccessfullProcessedEvent == null && e instanceof Record) {
                try {
                    maybeFlushAndMarkOffset((Record)e);
                } catch(Exception ex) {
                    logger.warn("flush offset error record={}", e);
                }
            }
        }
    }

    @Override
    public void initialize() throws Exception {
        File dir = new File(commitLogRelocationDir);
        if (!dir.exists()) {
            if (!dir.mkdir()) {
                throw new IOException("Failed to create " + commitLogRelocationDir);
            }
        }
        File archiveDir = new File(dir, ARCHIVE_FOLDER);
        if (!archiveDir.exists()) {
            if (!archiveDir.mkdir()) {
                throw new IOException("Failed to create " + archiveDir);
            }
        }
        File errorDir = new File(dir, ERROR_FOLDER);
        if (!errorDir.exists()) {
            if (!errorDir.mkdir()) {
                throw new IOException("Failed to create " + errorDir);
            }
        }
    }

    /**
     * Override destroy to clean up resources after stopping the processor
     */
    @Override
    public void close() throws PulsarClientException {
        producer.close();
        client.close();
    }

    void processEvent(Event event) throws PulsarClientException {
        switch (event.getEventType()) {
            case CHANGE_EVENT:
            case TOMBSTONE_EVENT:
                publishRecord((Record) event);
                notCommittedEvents++;
                break;
            case EOF_EVENT:
                EOFEvent eofEvent = (EOFEvent) event;
                String folder = eofEvent.success ? ARCHIVE_FOLDER : ERROR_FOLDER;
                CommitLogUtil.moveCommitLog(eofEvent.file, Paths.get(commitLogRelocationDir, folder));
                meterRegistry.counter("processedFiles").increment();
                break;
            default:
                throw new CassandraConnectorTaskException("Encountered unexpected record with type: " + event.getEventType());
        }
    }

    Record publishRecord(Record record) throws PulsarClientException {
        EventKey ek = new EventKey(record.source.nodeId,
                record.source.keyspaceTable.keyspace,
                record.source.keyspaceTable.table,
                record.rowData.primaryKeyValues());
        EventValue ev = new EventValue(record.source.timestamp.toEpochMilli(),
                record.source.nodeId, record.op);
        KeyValue<EventKey, EventValue> kv = new KeyValue<>(ek, ev);
        producer.send(kv);
        meterRegistry.counter("processedMutations").increment();
        logger.debug("Sending record key={} value={}", ek, ev);
        return record;
    }

    void maybeFlushAndMarkOffset(Record record) throws IOException {
        long now = System.currentTimeMillis();
        long timeSinceLastFlush = now - timeOfLastFlush;
        if (offsetFlushPolicy.shouldFlush(Duration.ofMillis(timeSinceLastFlush), notCommittedEvents)) {
            SourceInfo source = record.source;
            offsetWriter.markOffset(source.keyspaceTable.name(), source.commitLogPosition);
            offsetWriter.flush();
            this.meterRegistry.counter("commits").increment();
            this.meterRegistry.counter("committedEvents").increment(notCommittedEvents);
            notCommittedEvents = 0L;
            timeOfLastFlush = now;
            logger.debug("Offset flushed source="+source);
        }
    }
}
