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
 * Consumer statistics.
 * Provides metrics about message consumption.
 */
public interface ConsumerStats {
    
    /**
     * Get total number of messages received.
     * 
     * @return Messages received count
     */
    long getMessagesReceived();
    
    /**
     * Get total bytes received.
     * 
     * @return Bytes received
     */
    long getBytesReceived();
    
    /**
     * Get number of acknowledgments.
     * 
     * @return Acknowledgment count
     */
    long getAcknowledgments();
    
    /**
     * Get number of negative acknowledgments.
     * 
     * @return Negative acknowledgment count
     */
    long getNegativeAcknowledgments();
    
    /**
     * Get number of receive errors.
     * 
     * @return Receive error count
     */
    long getReceiveErrors();
    
    /**
     * Get receive throughput (messages per second).
     * 
     * @return Messages per second
     */
    double getReceiveThroughput();
    
    /**
     * Get average processing latency in milliseconds.
     * Time from receive to acknowledgment.
     * 
     * @return Average latency in ms
     */
    double getAverageProcessingLatencyMs();
}

// Made with Bob
