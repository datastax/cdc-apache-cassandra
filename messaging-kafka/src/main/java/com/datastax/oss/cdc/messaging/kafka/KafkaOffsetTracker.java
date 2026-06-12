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
package com.datastax.oss.cdc.messaging.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks Kafka offsets for manual acknowledgment semantics.
 * Maintains pending offsets per partition and commits them on acknowledgment.
 * 
 * <p>Thread-safe.
 */
public class KafkaOffsetTracker {
    
    private static final Logger log = LoggerFactory.getLogger(KafkaOffsetTracker.class);
    
    private final Consumer<?, ?> consumer;
    private final Map<TopicPartition, Long> pendingOffsets;
    private final Map<TopicPartition, Long> committedOffsets;
    
    /**
     * Create offset tracker for the given consumer.
     */
    public KafkaOffsetTracker(Consumer<?, ?> consumer) {
        this.consumer = consumer;
        this.pendingOffsets = new ConcurrentHashMap<>();
        this.committedOffsets = new ConcurrentHashMap<>();
    }
    
    /**
     * Track a message for acknowledgment.
     * Records the offset but doesn't commit yet.
     */
    public void track(String topic, int partition, long offset) {
        TopicPartition tp = new TopicPartition(topic, partition);
        pendingOffsets.put(tp, offset);
        
        log.trace("Tracked offset {} for partition {}", offset, tp);
    }
    
    /**
     * Acknowledge a message by committing its offset.
     * Commits the offset + 1 (next offset to read).
     */
    public void acknowledge(String topic, int partition, long offset) {
        TopicPartition tp = new TopicPartition(topic, partition);
        
        // Check if this offset is pending
        Long pendingOffset = pendingOffsets.get(tp);
        if (pendingOffset == null || pendingOffset != offset) {
            log.warn("Attempted to acknowledge non-pending offset {} for partition {}", 
                    offset, tp);
            return;
        }
        
        // Commit offset + 1 (next offset to read)
        long nextOffset = offset + 1;
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        offsetsToCommit.put(tp, new OffsetAndMetadata(nextOffset));
        
        try {
            consumer.commitSync(offsetsToCommit);
            committedOffsets.put(tp, nextOffset);
            pendingOffsets.remove(tp);
            
            log.debug("Committed offset {} for partition {}", nextOffset, tp);
        } catch (Exception e) {
            log.error("Failed to commit offset {} for partition {}", nextOffset, tp, e);
            throw new RuntimeException("Failed to commit offset", e);
        }
    }
    
    /**
     * Negative acknowledge a message by seeking back to its offset.
     * This will cause the message to be redelivered.
     */
    public void negativeAcknowledge(String topic, int partition, long offset) {
        TopicPartition tp = new TopicPartition(topic, partition);
        
        try {
            // Seek back to the offset to reprocess
            consumer.seek(tp, offset);
            pendingOffsets.remove(tp);
            
            log.debug("Negative acknowledged offset {} for partition {}, seeking back", 
                     offset, tp);
        } catch (Exception e) {
            log.error("Failed to negative acknowledge offset {} for partition {}", 
                     offset, tp, e);
            throw new RuntimeException("Failed to negative acknowledge", e);
        }
    }
    
    /**
     * Acknowledge all pending offsets up to and including the given offset.
     * Useful for batch acknowledgment.
     */
    public void acknowledgeCumulative(String topic, int partition, long offset) {
        TopicPartition tp = new TopicPartition(topic, partition);
        
        // Commit offset + 1 (next offset to read)
        long nextOffset = offset + 1;
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        offsetsToCommit.put(tp, new OffsetAndMetadata(nextOffset));
        
        try {
            consumer.commitSync(offsetsToCommit);
            committedOffsets.put(tp, nextOffset);
            
            // Remove all pending offsets up to this offset
            pendingOffsets.entrySet().removeIf(entry -> 
                entry.getKey().equals(tp) && entry.getValue() <= offset);
            
            log.debug("Cumulatively committed offset {} for partition {}", nextOffset, tp);
        } catch (Exception e) {
            log.error("Failed to cumulatively commit offset {} for partition {}", 
                     nextOffset, tp, e);
            throw new RuntimeException("Failed to commit offset", e);
        }
    }
    
    /**
     * Get the last committed offset for a partition.
     */
    public Long getCommittedOffset(String topic, int partition) {
        TopicPartition tp = new TopicPartition(topic, partition);
        return committedOffsets.get(tp);
    }
    
    /**
     * Get the pending offset for a partition.
     */
    public Long getPendingOffset(String topic, int partition) {
        TopicPartition tp = new TopicPartition(topic, partition);
        return pendingOffsets.get(tp);
    }
    
    /**
     * Check if an offset is pending acknowledgment.
     */
    public boolean isPending(String topic, int partition, long offset) {
        TopicPartition tp = new TopicPartition(topic, partition);
        Long pendingOffset = pendingOffsets.get(tp);
        return pendingOffset != null && pendingOffset == offset;
    }
    
    /**
     * Clear all pending offsets.
     */
    public void clearPending() {
        pendingOffsets.clear();
        log.debug("Cleared all pending offsets");
    }
    
    /**
     * Get the number of pending offsets.
     */
    public int getPendingCount() {
        return pendingOffsets.size();
    }
}

