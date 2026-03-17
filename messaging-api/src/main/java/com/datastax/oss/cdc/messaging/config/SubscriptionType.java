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
package com.datastax.oss.cdc.messaging.config;

/**
 * Subscription type for message consumers.
 * Defines how messages are distributed among consumers in a subscription.
 */
public enum SubscriptionType {
    /**
     * Exclusive subscription - only one consumer can subscribe.
     * All messages go to that consumer.
     */
    EXCLUSIVE,
    
    /**
     * Shared subscription - multiple consumers share messages.
     * Messages are distributed round-robin.
     */
    SHARED,
    
    /**
     * Key-shared subscription - messages with same key go to same consumer.
     * Maintains per-key ordering while allowing parallel processing.
     */
    KEY_SHARED,
    
    /**
     * Failover subscription - one active consumer with standby consumers.
     * If active fails, standby takes over.
     */
    FAILOVER
}

// Made with Bob
