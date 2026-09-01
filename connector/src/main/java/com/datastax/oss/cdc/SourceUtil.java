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

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Shared utilities for Pulsar and Kafka source connectors.
 */
@Slf4j
public final class SourceUtil {

    private SourceUtil() {}

    /**
     * Sleeps for an exponentially increasing randomised delay after a CQL availability failure,
     * then increments and returns the updated consecutive-failure counter.
     *
     * @param throwable                      the cause of the failure, used only for logging
     * @param consecutiveUnavailableException current consecutive-failure count (before this call)
     * @param config                          connector config supplying backoff bounds
     * @return the new consecutive-failure count (caller should store this)
     */
    public static long backoffRetry(Throwable throwable, long consecutiveUnavailableException,
                                    CassandraSourceConnectorConfig config) {
        consecutiveUnavailableException++;
        long maxWait = Math.min(config.getQueryMaxBackoffInSec() * 1000,
                config.getQueryBackoffInMs() << consecutiveUnavailableException);
        long pauseInMs = ThreadLocalRandom.current().nextLong(0, Math.max(1, maxWait));
        log.warn("CQL availability issue={}, consecutiveUnavailableException={}, pausing {}ms before retrying",
                throwable, consecutiveUnavailableException, pauseInMs);
        try {
            Thread.sleep(pauseInMs);
        } catch (InterruptedException ex) {
            log.warn("sleep interrupted:", ex);
        }
        return consecutiveUnavailableException;
    }
}
