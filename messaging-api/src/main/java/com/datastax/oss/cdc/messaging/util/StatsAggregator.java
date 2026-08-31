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
package com.datastax.oss.cdc.messaging.util;

import com.datastax.oss.cdc.messaging.stats.ConsumerStats;
import com.datastax.oss.cdc.messaging.stats.ProducerStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * Statistics aggregation utilities.
 * Aggregates metrics from multiple producers or consumers.
 * 
 * <p>Thread-safe utility class with static methods.
 */
public final class StatsAggregator {
    
    private static final Logger log = LoggerFactory.getLogger(StatsAggregator.class);
    
    private StatsAggregator() {
        // Utility class
    }
    
    /**
     * Aggregate producer statistics from multiple producers.
     * 
     * @param producerStats Collection of producer statistics
     * @return Aggregated statistics
     */
    public static AggregatedProducerStats aggregateProducerStats(
            Collection<ProducerStats> producerStats) {
        
        if (producerStats == null || producerStats.isEmpty()) {
            return new AggregatedProducerStats(0, 0, 0, 0.0, 0, 0.0);
        }
        
        long totalMessagesSent = 0;
        long totalBytesSent = 0;
        long totalSendErrors = 0;
        double totalLatency = 0;
        long totalPendingMessages = 0;
        double totalThroughput = 0;
        int count = 0;
        
        for (ProducerStats stats : producerStats) {
            if (stats != null) {
                totalMessagesSent += stats.getMessagesSent();
                totalBytesSent += stats.getBytesSent();
                totalSendErrors += stats.getSendErrors();
                totalLatency += stats.getAverageSendLatencyMs();
                totalPendingMessages += stats.getPendingMessages();
                totalThroughput += stats.getSendThroughput();
                count++;
            }
        }
        
        double avgLatency = count > 0 ? totalLatency / count : 0.0;
        
        log.debug("Aggregated {} producer stats: messages={}, bytes={}, errors={}, avgLatency={}ms",
            count, totalMessagesSent, totalBytesSent, totalSendErrors, avgLatency);
        
        return new AggregatedProducerStats(
            totalMessagesSent,
            totalBytesSent,
            totalSendErrors,
            avgLatency,
            totalPendingMessages,
            totalThroughput
        );
    }
    
    /**
     * Aggregate consumer statistics from multiple consumers.
     * 
     * @param consumerStats Collection of consumer statistics
     * @return Aggregated statistics
     */
    public static AggregatedConsumerStats aggregateConsumerStats(
            Collection<ConsumerStats> consumerStats) {
        
        if (consumerStats == null || consumerStats.isEmpty()) {
            return new AggregatedConsumerStats(0, 0, 0, 0, 0, 0.0, 0.0);
        }
        
        long totalMessagesReceived = 0;
        long totalBytesReceived = 0;
        long totalAcknowledgments = 0;
        long totalNegativeAcknowledgments = 0;
        long totalReceiveErrors = 0;
        double totalThroughput = 0;
        double totalLatency = 0;
        int count = 0;
        
        for (ConsumerStats stats : consumerStats) {
            if (stats != null) {
                totalMessagesReceived += stats.getMessagesReceived();
                totalBytesReceived += stats.getBytesReceived();
                totalAcknowledgments += stats.getAcknowledgments();
                totalNegativeAcknowledgments += stats.getNegativeAcknowledgments();
                totalReceiveErrors += stats.getReceiveErrors();
                totalThroughput += stats.getReceiveThroughput();
                totalLatency += stats.getAverageProcessingLatencyMs();
                count++;
            }
        }
        
        double avgLatency = count > 0 ? totalLatency / count : 0.0;
        
        log.debug("Aggregated {} consumer stats: messages={}, bytes={}, acks={}, nacks={}, errors={}, avgLatency={}ms",
            count, totalMessagesReceived, totalBytesReceived, totalAcknowledgments,
            totalNegativeAcknowledgments, totalReceiveErrors, avgLatency);
        
        return new AggregatedConsumerStats(
            totalMessagesReceived,
            totalBytesReceived,
            totalAcknowledgments,
            totalNegativeAcknowledgments,
            totalReceiveErrors,
            totalThroughput,
            avgLatency
        );
    }
    
    /**
     * Aggregated producer statistics snapshot.
     */
    public static class AggregatedProducerStats implements ProducerStats {
        private final long messagesSent;
        private final long bytesSent;
        private final long sendErrors;
        private final double averageSendLatencyMs;
        private final long pendingMessages;
        private final double sendThroughput;
        
        public AggregatedProducerStats(
                long messagesSent,
                long bytesSent,
                long sendErrors,
                double averageSendLatencyMs,
                long pendingMessages,
                double sendThroughput) {
            this.messagesSent = messagesSent;
            this.bytesSent = bytesSent;
            this.sendErrors = sendErrors;
            this.averageSendLatencyMs = averageSendLatencyMs;
            this.pendingMessages = pendingMessages;
            this.sendThroughput = sendThroughput;
        }
        
        @Override
        public long getMessagesSent() {
            return messagesSent;
        }
        
        @Override
        public long getBytesSent() {
            return bytesSent;
        }
        
        @Override
        public long getSendErrors() {
            return sendErrors;
        }
        
        @Override
        public double getAverageSendLatencyMs() {
            return averageSendLatencyMs;
        }
        
        @Override
        public long getPendingMessages() {
            return pendingMessages;
        }
        
        @Override
        public double getSendThroughput() {
            return sendThroughput;
        }
        
        @Override
        public String toString() {
            return String.format(
                "AggregatedProducerStats{messagesSent=%d, bytesSent=%d, sendErrors=%d, " +
                "avgLatency=%.2fms, pendingMessages=%d, throughput=%.2f msg/s}",
                messagesSent, bytesSent, sendErrors, averageSendLatencyMs,
                pendingMessages, sendThroughput);
        }
    }
    
    /**
     * Aggregated consumer statistics snapshot.
     */
    public static class AggregatedConsumerStats implements ConsumerStats {
        private final long messagesReceived;
        private final long bytesReceived;
        private final long acknowledgments;
        private final long negativeAcknowledgments;
        private final long receiveErrors;
        private final double receiveThroughput;
        private final double averageProcessingLatencyMs;
        
        public AggregatedConsumerStats(
                long messagesReceived,
                long bytesReceived,
                long acknowledgments,
                long negativeAcknowledgments,
                long receiveErrors,
                double receiveThroughput,
                double averageProcessingLatencyMs) {
            this.messagesReceived = messagesReceived;
            this.bytesReceived = bytesReceived;
            this.acknowledgments = acknowledgments;
            this.negativeAcknowledgments = negativeAcknowledgments;
            this.receiveErrors = receiveErrors;
            this.receiveThroughput = receiveThroughput;
            this.averageProcessingLatencyMs = averageProcessingLatencyMs;
        }
        
        @Override
        public long getMessagesReceived() {
            return messagesReceived;
        }
        
        @Override
        public long getBytesReceived() {
            return bytesReceived;
        }
        
        @Override
        public long getAcknowledgments() {
            return acknowledgments;
        }
        
        @Override
        public long getNegativeAcknowledgments() {
            return negativeAcknowledgments;
        }
        
        @Override
        public long getReceiveErrors() {
            return receiveErrors;
        }
        
        @Override
        public double getReceiveThroughput() {
            return receiveThroughput;
        }
        
        @Override
        public double getAverageProcessingLatencyMs() {
            return averageProcessingLatencyMs;
        }
        
        @Override
        public String toString() {
            return String.format(
                "AggregatedConsumerStats{messagesReceived=%d, bytesReceived=%d, " +
                "acks=%d, nacks=%d, receiveErrors=%d, throughput=%.2f msg/s, avgLatency=%.2fms}",
                messagesReceived, bytesReceived, acknowledgments, negativeAcknowledgments,
                receiveErrors, receiveThroughput, averageProcessingLatencyMs);
        }
    }
    
    /**
     * Calculate success rate for producer.
     * 
     * @param stats Producer statistics
     * @return Success rate (0.0 to 1.0)
     */
    public static double calculateProducerSuccessRate(ProducerStats stats) {
        if (stats == null) {
            return 0.0;
        }
        
        long total = stats.getMessagesSent() + stats.getSendErrors();
        if (total == 0) {
            return 1.0; // No messages sent yet, consider 100% success
        }
        
        return (double) stats.getMessagesSent() / total;
    }
    
    /**
     * Calculate success rate for consumer.
     * 
     * @param stats Consumer statistics
     * @return Success rate (0.0 to 1.0)
     */
    public static double calculateConsumerSuccessRate(ConsumerStats stats) {
        if (stats == null) {
            return 0.0;
        }
        
        long total = stats.getMessagesReceived() + stats.getReceiveErrors();
        if (total == 0) {
            return 1.0; // No messages received yet, consider 100% success
        }
        
        return (double) stats.getMessagesReceived() / total;
    }
    
    /**
     * Calculate acknowledgment rate for consumer.
     * 
     * @param stats Consumer statistics
     * @return Acknowledgment rate (0.0 to 1.0)
     */
    public static double calculateAcknowledgmentRate(ConsumerStats stats) {
        if (stats == null) {
            return 0.0;
        }
        
        long total = stats.getAcknowledgments() + stats.getNegativeAcknowledgments();
        if (total == 0) {
            return 1.0; // No acknowledgments yet
        }
        
        return (double) stats.getAcknowledgments() / total;
    }
}

