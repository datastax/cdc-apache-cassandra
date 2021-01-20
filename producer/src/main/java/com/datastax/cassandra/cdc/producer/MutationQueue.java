package com.datastax.cassandra.cdc.producer;

import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import io.debezium.util.ObjectSizeCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A queue which serves as handover point between producer threads (e.g. MySQL's
 * binlog reader thread) and the Kafka Connect polling loop.
 * <p>
 * The queue is configurable in different aspects, e.g. its maximum size and the
 * time to sleep (block) between two subsequent poll calls. The queue applies back-pressure
 * semantics, i.e. if it holds the maximum number of elements, subsequent calls
 * to enqueue(Object) will block until elements have been removed from
 * the queue.
 * <p>
 *
 * @author Gunnar Morling

 */
@Singleton
public class MutationQueue {

    private static final Logger logger = LoggerFactory.getLogger(MutationQueue.class);

    private final CassandraConnectorConfiguration config;
    private final Duration pollInterval;
    private final int maxBatchSize;
    private final int maxQueueSize;
    private final long maxQueueSizeInBytes;
    private final BlockingQueue<Mutation> queue;
    private final Metronome metronome;
    //private final Supplier<PreviousContext> loggingContextSupplier;
    private AtomicLong currentQueueSizeInBytes = new AtomicLong(0);
    private Map<Mutation, Long> objectMap = new ConcurrentHashMap<>();

    // last enqueue event position
    volatile long segment = 0;
    volatile int position = 0;

    public MutationQueue(CassandraConnectorConfiguration config) {
        this.config = config;
        this.pollInterval = config.pollIntervalMs;
        this.maxBatchSize = config.maxBatchSize;
        this.maxQueueSize = config.maxQueueSize;
        this.queue = new LinkedBlockingDeque<>(maxQueueSize);
        this.metronome = Metronome.sleeper(pollInterval, Clock.SYSTEM);
        this.maxQueueSizeInBytes = config.maxQueueSizeInBytes;
    }

    public void enqueue(Mutation mutation) throws InterruptedException {
        if (mutation == null || mutation.getSegment() < this.segment || (mutation.getSegment() == this.segment && mutation.getPosition() <= this.position)) {
            logger.debug("seg/pos={}/{} Ignoring mutation={}", this.segment, this.position, mutation);
            return;
        }

        // The calling thread has been interrupted, let's abort
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }

        logger.debug("Enqueuing mutation={}", mutation);

        // Waiting for queue to add more record.
        while (maxQueueSizeInBytes > 0 && currentQueueSizeInBytes.get() > maxQueueSizeInBytes) {
            Thread.sleep(pollInterval.toMillis());
        }

        // this will also raise an InterruptedException if the thread is interrupted while waiting for space in the queue
        this.queue.put(mutation);
        this.segment = mutation.getSegment();
        this.position = mutation.getPosition();

        // If we pass a positiveLong max.queue.size.in.bytes to enable handling queue size in bytes feature
        if (maxQueueSizeInBytes > 0) {
            long messageSize = ObjectSizeCalculator.getObjectSize(mutation);
            objectMap.put(mutation, messageSize);
            currentQueueSizeInBytes.addAndGet(messageSize);
        }
    }

    public Mutation take() throws InterruptedException {
        Mutation m = queue.take();
        logger.debug("m={}", m);
        return m;
    }
}

