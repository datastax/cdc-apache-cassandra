package com.datastax.cassandra.cdc.producer;

import io.debezium.time.Temporals;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import io.debezium.util.ObjectSizeCalculator;
import io.debezium.util.Threads;
import io.debezium.util.Threads.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
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
public class ChangeEventQueue  {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeEventQueue.class);

    private final CassandraConnectorConfiguration config;
    private final Duration pollInterval;
    private final int maxBatchSize;
    private final int maxQueueSize;
    private final long maxQueueSizeInBytes;
    private final BlockingQueue<Event> queue;
    private final Metronome metronome;
    //private final Supplier<PreviousContext> loggingContextSupplier;
    private AtomicLong currentQueueSizeInBytes = new AtomicLong(0);
    private Map<Event, Long> objectMap = new ConcurrentHashMap<>();

    private volatile RuntimeException producerException;

    public ChangeEventQueue(CassandraConnectorConfiguration config) {
        this.config = config;
        this.pollInterval = config.pollIntervalMs;
        this.maxBatchSize = config.maxBatchSize;
        this.maxQueueSize = config.maxQueueSize;
        this.queue = new LinkedBlockingDeque<>(maxQueueSize);
        this.metronome = Metronome.sleeper(pollInterval, Clock.SYSTEM);
        this.maxQueueSizeInBytes = config.maxQueueSizeInBytes;
    }

    public void enqueue(Event event) throws InterruptedException {
        if (event == null) {
            return;
        }

        // The calling thread has been interrupted, let's abort
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Enqueuing source record '{}'", event);
        }
        // Waiting for queue to add more record.
        while (maxQueueSizeInBytes > 0 && currentQueueSizeInBytes.get() > maxQueueSizeInBytes) {
            Thread.sleep(pollInterval.toMillis());
        }
        // this will also raise an InterruptedException if the thread is interrupted while waiting for space in the queue
        queue.put(event);
        // If we pass a positiveLong max.queue.size.in.bytes to enable handling queue size in bytes feature
        if (maxQueueSizeInBytes > 0) {
            long messageSize = ObjectSizeCalculator.getObjectSize(event);
            objectMap.put(event, messageSize);
            currentQueueSizeInBytes.addAndGet(messageSize);
        }
    }

    public Event take() throws InterruptedException {
        return queue.take();
    }

    public void producerException(final RuntimeException producerException) {
        this.producerException = producerException;
    }

    private void throwProducerExceptionIfPresent() {
        if (producerException != null) {
            throw producerException;
        }
    }

    public int totalCapacity() {
        return maxQueueSize;
    }

    public int remainingCapacity() {
        return queue.remainingCapacity();
    }

    public long maxQueueSizeInBytes() {
        return maxQueueSizeInBytes;
    }

    public long currentQueueSizeInBytes() {
        return currentQueueSizeInBytes.get();
    }
}

