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
package com.datastax.oss.cdc.messaging.stats;

/**
 * Producer statistics.
 * Provides metrics about message production.
 */
public interface ProducerStats {
    
    /**
     * Get total number of messages sent.
     * 
     * @return Messages sent count
     */
    long getMessagesSent();
    
    /**
     * Get total bytes sent.
     * 
     * @return Bytes sent
     */
    long getBytesSent();
    
    /**
     * Get number of send errors.
     * 
     * @return Send error count
     */
    long getSendErrors();
    
    /**
     * Get average send latency in milliseconds.
     * 
     * @return Average latency in ms
     */
    double getAverageSendLatencyMs();
    
    /**
     * Get number of pending messages.
     * 
     * @return Pending message count
     */
    long getPendingMessages();
    
    /**
     * Get send throughput (messages per second).
     * 
     * @return Messages per second
     */
    double getSendThroughput();
}

// Made with Bob
