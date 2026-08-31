/**
 * Copyright DataStax, Inc 2021.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.cdc;

import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Adaptive-concurrency CQL read-back executor pool shared by the Pulsar and Kafka source
 * connectors: single-threaded-per-key executors (to serialize same-key reads so the mutation
 * cache stays effective), a pool size that grows/shrinks with the observed mobile-average CQL
 * latency, and exponential backoff-and-retry on Cassandra unavailability.
 * <p>
 * TODO: growing/shrinking the pool by adding/removing single-threaded executors is expensive
 * (thread creation cost, back-and-forth churn under fluctuating latency) and shifts the
 * key-hash-to-thread mapping on every resize, so a key's queries can briefly interleave across
 * two executors during a resize. Consider replacing this with a fixed-size thread pool guarded
 * by a semaphore whose permit count is adjusted dynamically based on latency instead.
 */
@Slf4j
public class AdaptiveQueryExecutor {

    private final CassandraSourceConnectorConfig config;
    private final List<ExecutorService> queryExecutors;

    private final AtomicLong batchTotalLatency = new AtomicLong(0);
    private final AtomicLong batchTotalQuery = new AtomicLong(0);
    private final long[] batchAvgLatencyMovingWindow = new long[10];
    private int batchAvgLatencyHead = 0;
    private int batchAvgLatencyMovingWindowSize = 0;
    private long consecutiveUnavailableException = 0;

    public AdaptiveQueryExecutor(CassandraSourceConnectorConfig config) {
        this.config = config;
        log.info("initQueryExecutors with {} threads", config.getQueryExecutors());
        this.queryExecutors = new ArrayList<>(config.getQueryExecutors());
        for (int i = 0; i < config.getQueryExecutors(); i++) {
            this.queryExecutors.add(Executors.newSingleThreadExecutor());
        }
    }

    /**
     * Resets the per-batch latency/query counters. Call once at the start of each poll/read
     * batch, before submitting any queries via {@link #executeOrdered}.
     */
    public void beginBatch() {
        batchTotalLatency.set(0);
        batchTotalQuery.set(0);
    }

    /**
     * Submits a task on the single-threaded executor selected by {@code key}'s hash, so that
     * all queries for the same mutation key are processed in submission order.
     */
    public synchronized <T> Future<T> executeOrdered(Object key, Callable<T> task) {
        Preconditions.checkArgument(key != null, "message key should not be null");
        int threadIdx = (Objects.hashCode(key) & Integer.MAX_VALUE) % queryExecutors.size();
        if (log.isDebugEnabled()) {
            log.debug("Submit task key={} on thread={}/{}", key, threadIdx, queryExecutors.size());
        }
        return queryExecutors.get(threadIdx).submit(task);
    }

    /**
     * Records one successful CQL query's latency towards this batch's mobile average.
     */
    public void recordQueryLatency(long durationMs) {
        batchTotalLatency.addAndGet(durationMs);
        batchTotalQuery.incrementAndGet();
    }

    /**
     * Grows or shrinks the executor pool based on this batch's mobile-average latency, if any
     * queries were recorded via {@link #recordQueryLatency}. Call once at the end of each batch.
     */
    public synchronized void maybeAdjust() {
        if (batchTotalQuery.get() > 0) {
            adjustExecutors();
        }
    }

    private void adjustExecutors() {
        long batchAvgLatency = this.batchTotalLatency.get() / this.batchTotalQuery.get();
        this.batchAvgLatencyMovingWindow[this.batchAvgLatencyHead] = batchAvgLatency;
        this.batchAvgLatencyHead = (this.batchAvgLatencyHead + 1) % this.batchAvgLatencyMovingWindow.length;
        this.batchAvgLatencyMovingWindowSize = Math.min(batchAvgLatencyMovingWindowSize + 1, this.batchAvgLatencyMovingWindow.length);

        long latencyTotal = 0;
        for (int i = 0; i < this.batchAvgLatencyMovingWindowSize; i++) {
            if (log.isDebugEnabled()) {
                log.debug("batchAvgLatencyMovingWindow={}, batchAvgLatencyHead={}, batchAvgLatencyMovingWindowSize={}, i={}",
                        Arrays.toString(batchAvgLatencyMovingWindow), batchAvgLatencyHead, batchAvgLatencyMovingWindowSize, i);
            }
            latencyTotal += this.batchAvgLatencyMovingWindow[i];
        }
        long mobileAvgLatency = latencyTotal / batchAvgLatencyMovingWindowSize;
        if (log.isDebugEnabled()) {
            log.debug("mobileAvgLatency={}, batchAvgLatencyMovingWindow={}", mobileAvgLatency, Arrays.toString(batchAvgLatencyMovingWindow));
        }
        if (mobileAvgLatency < config.getQueryMinMobileAvgLatency() && queryExecutors.size() < config.getQueryExecutors()) {
            queryExecutors.add(Executors.newSingleThreadExecutor());
            log.info("mobileAvgLatency={}, increasing the query executor to {} threads", mobileAvgLatency, queryExecutors.size());
        }
        if (mobileAvgLatency > config.getQueryMaxMobileAvgLatency() && queryExecutors.size() > 1) {
            queryExecutors.remove(queryExecutors.size() - 1).shutdown();
            log.info("mobileAvgLatency={}, decreasing the query executor to {} threads", mobileAvgLatency, queryExecutors.size());
        }
    }

    /**
     * Decreases the executor pool by ~10% (at least one thread) in response to a CQL read
     * failure (e.g. read timeout, overload), independent of the mobile-average adjustment.
     */
    public synchronized void decreaseOnError(Throwable throwable) {
        if (queryExecutors.size() > 1) {
            int numberOfThreadToRemove = Math.max(1, queryExecutors.size() / 10);
            for (int i = 0; i < numberOfThreadToRemove; i++) {
                queryExecutors.remove(queryExecutors.size() - 1).shutdown();
            }
            log.warn("CQL read issue={}, decreasing the query executor to {} threads", throwable, queryExecutors.size());
        } else {
            log.warn("CQL read issue={} with only 1 executor threads, please consider limiting the source connector throughput to avoid overloading the Cassandra cluster", throwable);
        }
    }

    private long waitInMs(long attempt) {
        return Math.min(config.getQueryMaxBackoffInSec() * 1000, config.getQueryBackoffInMs() << attempt);
    }

    private long randomWaitInMs(long attempt) {
        return ThreadLocalRandom.current().nextLong(0, waitInMs(attempt));
    }

    /**
     * Sleeps for an exponentially increasing (randomized) backoff, tracking the number of
     * consecutive unavailability errors seen since the last {@link #resetBackoff}.
     */
    public void backoffRetry(Throwable throwable) {
        consecutiveUnavailableException++;
        long pauseInMs = randomWaitInMs(consecutiveUnavailableException);
        log.warn("CQL availability issue={}, consecutiveUnavailableException={}, pausing {}ms before retrying",
                throwable, consecutiveUnavailableException, pauseInMs);
        try {
            Thread.sleep(pauseInMs);
        } catch (InterruptedException ex) {
            log.warn("sleep interrupted:", ex);
        }
    }

    /**
     * Resets the consecutive-unavailability counter. Call after a successfully completed batch.
     */
    public void resetBackoff() {
        consecutiveUnavailableException = 0;
    }

    public synchronized int size() {
        return queryExecutors.size();
    }

    public synchronized void shutdown() {
        for (ExecutorService thread : queryExecutors) {
            thread.shutdownNow();
        }
    }
}
