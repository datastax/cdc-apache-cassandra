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

import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.google.common.base.Preconditions;
import io.vavr.Tuple2;
import lombok.extern.slf4j.Slf4j;

import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Shared utilities for Pulsar and Kafka source connectors.
 */
@Slf4j
public final class SourceUtil {

    private SourceUtil() {}

    private static final long INIT_RETRY_MAX_SINGLE_WAIT_MS = 5000L;

    /**
     * Creates a {@link CassandraClient}, validates that the configured keyspace and table exist,
     * and returns the client. Caller is responsible for converter setup after this returns.
     *
     * @param config          connector config
     * @param applicationName driver application-name tag (connector-specific)
     * @param listener        schema-change listener (typically {@code this} in the calling class)
     * @return an initialised {@link CassandraClient} whose keyspace and table have been verified
     */
    public static CassandraClient initCassandraClient(CassandraSourceConnectorConfig config,
                                                      String applicationName,
                                                      SchemaChangeListener listener) {
        CassandraClient client = new CassandraClient(config, Version.getVersion(), applicationName, listener);
        Tuple2<KeyspaceMetadata, TableMetadata> tuple =
                client.getTableMetadata(config.getKeyspaceName(), config.getTableName());
        Preconditions.checkArgument(tuple._1 != null,
                String.format(Locale.ROOT, "Keyspace %s does not exist", config.getKeyspaceName()));
        Preconditions.checkArgument(tuple._2 != null,
                String.format(Locale.ROOT, "Table %s.%s does not exist",
                        config.getKeyspaceName(), config.getTableName()));
        return client;
    }

    /**
     * Calls {@link #initCassandraClient} in a retry loop with jittered backoff until either the
     * call succeeds or the deadline derived from {@code config.getQueryMaxBackoffInSec()} is exceeded.
     *
     * @param config          connector config supplying backoff bounds
     * @param applicationName driver application-name tag (connector-specific)
     * @param listener        schema-change listener (typically {@code this} in the calling class)
     * @return an initialised {@link CassandraClient}
     * @throws RuntimeException wrapping the last failure if the deadline is exceeded
     */
    public static CassandraClient initCassandraClientWithRetry(CassandraSourceConnectorConfig config,
                                                               String applicationName,
                                                               SchemaChangeListener listener) {
        long consecutiveFailures = 0;
        long deadlineMs = System.currentTimeMillis() + config.getQueryMaxBackoffInSec() * 1000;
        while (true) {
            try {
                return initCassandraClient(config, applicationName, listener);
            } catch (Throwable err) {
                if (System.currentTimeMillis() >= deadlineMs) {
                    throw new RuntimeException("Failed to initialize Cassandra client after " +
                            config.getQueryMaxBackoffInSec() + "s, giving up", err);
                }
                consecutiveFailures = backoffRetry(err, consecutiveFailures, config,
                        INIT_RETRY_MAX_SINGLE_WAIT_MS);
            }
        }
    }

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
        return backoffRetry(throwable, consecutiveUnavailableException, config,
                config.getQueryMaxBackoffInSec() * 1000);
    }

    /**
     * Same jittered-backoff strategy as above, but caps each individual sleep at
     * {@code maxSingleWaitMs} instead of letting it grow all the way to {@code
     * query.maxBackoffInSec}. Callers whose caller can be killed for going unresponsive too long
     * (e.g. a Pulsar function instance, which is restarted if it doesn't yield control within its
     * configured health-check interval) need to keep retrying frequently even while an overall
     * give-up deadline measured in minutes/hours is still far off.
     */
    public static long backoffRetry(Throwable throwable, long consecutiveUnavailableException,
                                    CassandraSourceConnectorConfig config, long maxSingleWaitMs) {
        consecutiveUnavailableException++;
        long maxWait = Math.min(maxSingleWaitMs,
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
