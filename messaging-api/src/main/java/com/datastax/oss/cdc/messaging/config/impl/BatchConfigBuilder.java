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
package com.datastax.oss.cdc.messaging.config.impl;

import com.datastax.oss.cdc.messaging.config.BatchConfig;

import java.util.Objects;

/**
 * Builder for BatchConfig.
 * Provides fluent API for constructing immutable batch configuration.
 * 
 * <p>Usage:
 * <pre>{@code
 * BatchConfig config = BatchConfig.builder()
 *     .enabled(true)
 *     .maxMessages(1000)
 *     .maxBytes(128 * 1024)
 *     .maxDelayMs(10)
 *     .build();
 * }</pre>
 */
public class BatchConfigBuilder {
    
    private boolean enabled = false;
    private int maxMessages = 1000;
    private int maxBytes = 128 * 1024; // 128 KB
    private long maxDelayMs = 10;
    private boolean keyBasedBatching = false;
    
    private BatchConfigBuilder() {
    }
    
    /**
     * Create a new builder.
     * 
     * @return Builder instance
     */
    public static BatchConfigBuilder builder() {
        return new BatchConfigBuilder();
    }
    
    /**
     * Enable or disable batching.
     * 
     * @param enabled true to enable batching
     * @return This builder
     */
    public BatchConfigBuilder enabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }
    
    /**
     * Set maximum number of messages per batch.
     * 
     * @param maxMessages Max messages (must be > 0)
     * @return This builder
     */
    public BatchConfigBuilder maxMessages(int maxMessages) {
        if (maxMessages <= 0) {
            throw new IllegalArgumentException("Max messages must be > 0");
        }
        this.maxMessages = maxMessages;
        return this;
    }
    
    /**
     * Set maximum batch size in bytes.
     * 
     * @param maxBytes Max bytes (must be > 0)
     * @return This builder
     */
    public BatchConfigBuilder maxBytes(int maxBytes) {
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("Max bytes must be > 0");
        }
        this.maxBytes = maxBytes;
        return this;
    }
    
    /**
     * Set maximum delay before sending batch.
     * 
     * @param maxDelayMs Max delay in milliseconds (must be >= 0)
     * @return This builder
     */
    public BatchConfigBuilder maxDelayMs(long maxDelayMs) {
        if (maxDelayMs < 0) {
            throw new IllegalArgumentException("Max delay must be >= 0");
        }
        this.maxDelayMs = maxDelayMs;
        return this;
    }
    
    /**
     * Enable or disable key-based batching.
     * 
     * @param keyBasedBatching true for key-based batching
     * @return This builder
     */
    public BatchConfigBuilder keyBasedBatching(boolean keyBasedBatching) {
        this.keyBasedBatching = keyBasedBatching;
        return this;
    }
    
    /**
     * Build the BatchConfig.
     * 
     * @return Immutable BatchConfig instance
     */
    public BatchConfig build() {
        return new BatchConfigImpl(enabled, maxMessages, maxBytes, maxDelayMs, keyBasedBatching);
    }
    
    /**
     * Immutable implementation of BatchConfig.
     */
    private static class BatchConfigImpl implements BatchConfig {
        private final boolean enabled;
        private final int maxMessages;
        private final int maxBytes;
        private final long maxDelayMs;
        private final boolean keyBasedBatching;
        
        BatchConfigImpl(boolean enabled, int maxMessages, int maxBytes, 
                       long maxDelayMs, boolean keyBasedBatching) {
            this.enabled = enabled;
            this.maxMessages = maxMessages;
            this.maxBytes = maxBytes;
            this.maxDelayMs = maxDelayMs;
            this.keyBasedBatching = keyBasedBatching;
        }
        
        @Override
        public boolean isEnabled() {
            return enabled;
        }
        
        @Override
        public int getMaxMessages() {
            return maxMessages;
        }
        
        @Override
        public int getMaxBytes() {
            return maxBytes;
        }
        
        @Override
        public long getMaxDelayMs() {
            return maxDelayMs;
        }
        
        @Override
        public boolean isKeyBasedBatching() {
            return keyBasedBatching;
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BatchConfigImpl that = (BatchConfigImpl) o;
            return enabled == that.enabled &&
                   maxMessages == that.maxMessages &&
                   maxBytes == that.maxBytes &&
                   maxDelayMs == that.maxDelayMs &&
                   keyBasedBatching == that.keyBasedBatching;
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(enabled, maxMessages, maxBytes, maxDelayMs, keyBasedBatching);
        }
        
        @Override
        public String toString() {
            return "BatchConfig{" +
                    "enabled=" + enabled +
                    ", maxMessages=" + maxMessages +
                    ", maxBytes=" + maxBytes +
                    ", maxDelayMs=" + maxDelayMs +
                    ", keyBasedBatching=" + keyBasedBatching +
                    '}';
        }
    }
}

