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
package com.datastax.oss.cdc.messaging.stats.impl;

import com.datastax.oss.cdc.messaging.stats.ProducerStats;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Thread-safe implementation of ProducerStats.
 * Uses atomic counters and adders for high-performance concurrent updates.
 */
public class BaseProducerStats implements ProducerStats {
    
    private final LongAdder messagesSent = new LongAdder();
    private final LongAdder bytesSent = new LongAdder();
    private final LongAdder sendErrors = new LongAdder();
    private final AtomicLong pendingMessages = new AtomicLong(0);
    private final AtomicLong totalLatencyMs = new AtomicLong(0);
    private final AtomicLong latencySamples = new AtomicLong(0);
    private final AtomicLong startTimeMs = new AtomicLong(System.currentTimeMillis());
    
    @Override
    public long getMessagesSent() {
        return messagesSent.sum();
    }
    
    @Override
    public long getBytesSent() {
        return bytesSent.sum();
    }
    
    @Override
    public long getSendErrors() {
        return sendErrors.sum();
    }
    
    @Override
    public double getAverageSendLatencyMs() {
        long samples = latencySamples.get();
        if (samples == 0) {
            return 0.0;
        }
        return (double) totalLatencyMs.get() / samples;
    }
    
    @Override
    public long getPendingMessages() {
        return pendingMessages.get();
    }
    
    @Override
    public double getSendThroughput() {
        long elapsedMs = System.currentTimeMillis() - startTimeMs.get();
        if (elapsedMs == 0) {
            return 0.0;
        }
        return (double) messagesSent.sum() / (elapsedMs / 1000.0);
    }
    
    /**
     * Record a successful send.
     * 
     * @param bytes Number of bytes sent
     * @param latencyMs Send latency in milliseconds
     */
    public void recordSend(long bytes, long latencyMs) {
        messagesSent.increment();
        bytesSent.add(bytes);
        totalLatencyMs.addAndGet(latencyMs);
        latencySamples.incrementAndGet();
    }
    
    /**
     * Record a send error.
     */
    public void recordSendError() {
        sendErrors.increment();
    }
    
    /**
     * Increment pending messages count.
     */
    public void incrementPendingMessages() {
        pendingMessages.incrementAndGet();
    }
    
    /**
     * Decrement pending messages count.
     */
    public void decrementPendingMessages() {
        pendingMessages.decrementAndGet();
    }
    
    /**
     * Set pending messages count.
     * 
     * @param count Pending message count
     */
    public void setPendingMessages(long count) {
        pendingMessages.set(count);
    }
    
    /**
     * Reset all statistics.
     */
    public void reset() {
        messagesSent.reset();
        bytesSent.reset();
        sendErrors.reset();
        pendingMessages.set(0);
        totalLatencyMs.set(0);
        latencySamples.set(0);
        startTimeMs.set(System.currentTimeMillis());
    }
    
    @Override
    public String toString() {
        return "ProducerStats{" +
                "messagesSent=" + messagesSent.sum() +
                ", bytesSent=" + bytesSent.sum() +
                ", sendErrors=" + sendErrors.sum() +
                ", avgLatencyMs=" + String.format("%.2f", getAverageSendLatencyMs()) +
                ", pendingMessages=" + pendingMessages.get() +
                ", throughput=" + String.format("%.2f", getSendThroughput()) + " msg/s" +
                '}';
    }
}

// Made with Bob