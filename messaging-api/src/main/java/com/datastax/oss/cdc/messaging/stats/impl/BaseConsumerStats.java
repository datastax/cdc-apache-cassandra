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

import com.datastax.oss.cdc.messaging.stats.ConsumerStats;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Thread-safe implementation of ConsumerStats.
 * Uses atomic counters and adders for high-performance concurrent updates.
 */
public class BaseConsumerStats implements ConsumerStats {
    
    private final LongAdder messagesReceived = new LongAdder();
    private final LongAdder bytesReceived = new LongAdder();
    private final LongAdder acknowledgments = new LongAdder();
    private final LongAdder negativeAcknowledgments = new LongAdder();
    private final LongAdder receiveErrors = new LongAdder();
    private final AtomicLong totalProcessingLatencyMs = new AtomicLong(0);
    private final AtomicLong latencySamples = new AtomicLong(0);
    private final AtomicLong startTimeMs = new AtomicLong(System.currentTimeMillis());
    
    @Override
    public long getMessagesReceived() {
        return messagesReceived.sum();
    }
    
    @Override
    public long getBytesReceived() {
        return bytesReceived.sum();
    }
    
    @Override
    public long getAcknowledgments() {
        return acknowledgments.sum();
    }
    
    @Override
    public long getNegativeAcknowledgments() {
        return negativeAcknowledgments.sum();
    }
    
    @Override
    public long getReceiveErrors() {
        return receiveErrors.sum();
    }
    
    @Override
    public double getReceiveThroughput() {
        long elapsedMs = System.currentTimeMillis() - startTimeMs.get();
        if (elapsedMs == 0) {
            return 0.0;
        }
        return (double) messagesReceived.sum() / (elapsedMs / 1000.0);
    }
    
    @Override
    public double getAverageProcessingLatencyMs() {
        long samples = latencySamples.get();
        if (samples == 0) {
            return 0.0;
        }
        return (double) totalProcessingLatencyMs.get() / samples;
    }
    
    /**
     * Record a received message.
     * 
     * @param bytes Number of bytes received
     */
    public void recordReceive(long bytes) {
        messagesReceived.increment();
        bytesReceived.add(bytes);
    }
    
    /**
     * Record an acknowledgment.
     * 
     * @param processingLatencyMs Processing latency in milliseconds
     */
    public void recordAcknowledgment(long processingLatencyMs) {
        acknowledgments.increment();
        totalProcessingLatencyMs.addAndGet(processingLatencyMs);
        latencySamples.incrementAndGet();
    }
    
    /**
     * Record a negative acknowledgment.
     */
    public void recordNegativeAcknowledgment() {
        negativeAcknowledgments.increment();
    }
    
    /**
     * Record a receive error.
     */
    public void recordReceiveError() {
        receiveErrors.increment();
    }
    
    /**
     * Reset all statistics.
     */
    public void reset() {
        messagesReceived.reset();
        bytesReceived.reset();
        acknowledgments.reset();
        negativeAcknowledgments.reset();
        receiveErrors.reset();
        totalProcessingLatencyMs.set(0);
        latencySamples.set(0);
        startTimeMs.set(System.currentTimeMillis());
    }
    
    @Override
    public String toString() {
        return "ConsumerStats{" +
                "messagesReceived=" + messagesReceived.sum() +
                ", bytesReceived=" + bytesReceived.sum() +
                ", acknowledgments=" + acknowledgments.sum() +
                ", negativeAcknowledgments=" + negativeAcknowledgments.sum() +
                ", receiveErrors=" + receiveErrors.sum() +
                ", avgProcessingLatencyMs=" + String.format("%.2f", getAverageProcessingLatencyMs()) +
                ", throughput=" + String.format("%.2f", getReceiveThroughput()) + " msg/s" +
                '}';
    }
}

// Made with Bob