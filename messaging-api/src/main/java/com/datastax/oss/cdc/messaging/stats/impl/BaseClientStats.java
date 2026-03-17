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

import com.datastax.oss.cdc.messaging.stats.ClientStats;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe implementation of ClientStats.
 * Uses atomic counters for concurrent updates.
 */
public class BaseClientStats implements ClientStats {
    
    private final AtomicLong connectionCount = new AtomicLong(0);
    private final AtomicLong reconnectionCount = new AtomicLong(0);
    private final AtomicLong connectionFailures = new AtomicLong(0);
    private final AtomicLong producerCount = new AtomicLong(0);
    private final AtomicLong consumerCount = new AtomicLong(0);
    
    @Override
    public long getConnectionCount() {
        return connectionCount.get();
    }
    
    @Override
    public long getReconnectionCount() {
        return reconnectionCount.get();
    }
    
    @Override
    public long getConnectionFailures() {
        return connectionFailures.get();
    }
    
    @Override
    public long getProducerCount() {
        return producerCount.get();
    }
    
    @Override
    public long getConsumerCount() {
        return consumerCount.get();
    }
    
    /**
     * Increment connection count.
     */
    public void incrementConnectionCount() {
        connectionCount.incrementAndGet();
    }
    
    /**
     * Decrement connection count.
     */
    public void decrementConnectionCount() {
        connectionCount.decrementAndGet();
    }
    
    /**
     * Increment reconnection count.
     */
    public void incrementReconnectionCount() {
        reconnectionCount.incrementAndGet();
    }
    
    /**
     * Increment connection failure count.
     */
    public void incrementConnectionFailures() {
        connectionFailures.incrementAndGet();
    }
    
    /**
     * Increment producer count.
     */
    public void incrementProducerCount() {
        producerCount.incrementAndGet();
    }
    
    /**
     * Decrement producer count.
     */
    public void decrementProducerCount() {
        producerCount.decrementAndGet();
    }
    
    /**
     * Increment consumer count.
     */
    public void incrementConsumerCount() {
        consumerCount.incrementAndGet();
    }
    
    /**
     * Decrement consumer count.
     */
    public void decrementConsumerCount() {
        consumerCount.decrementAndGet();
    }
    
    /**
     * Reset all statistics.
     */
    public void reset() {
        connectionCount.set(0);
        reconnectionCount.set(0);
        connectionFailures.set(0);
        producerCount.set(0);
        consumerCount.set(0);
    }
    
    @Override
    public String toString() {
        return "ClientStats{" +
                "connections=" + connectionCount.get() +
                ", reconnections=" + reconnectionCount.get() +
                ", failures=" + connectionFailures.get() +
                ", producers=" + producerCount.get() +
                ", consumers=" + consumerCount.get() +
                '}';
    }
}

// Made with Bob