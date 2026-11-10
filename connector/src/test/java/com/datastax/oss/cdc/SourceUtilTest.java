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

import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class SourceUtilTest {

    private static CassandraSourceConnectorConfig config(Map<String, String> extra) {
        Map<String, String> props = new HashMap<>();
        props.put(CassandraSourceConnectorConfig.KEYSPACE_NAME_CONFIG, "ks1");
        props.put(CassandraSourceConnectorConfig.TABLE_NAME_CONFIG, "t1");
        props.put(CassandraSourceConnectorConfig.EVENTS_TOPIC_NAME_CONFIG, "events-ks1.t1");
        props.put(CassandraSourceConnectorConfig.QUERY_BACKOFF_IN_MS_CONFIG, "10");
        props.putAll(extra);
        return new CassandraSourceConnectorConfig(props);
    }

    @Test
    void backoffIncrementsTheFailureCount() {
        CassandraSourceConnectorConfig config = config(new HashMap<>());
        assertEquals(1, SourceUtil.backoffRetry(new RuntimeException("unavailable"), 0, config, 5));
        assertEquals(4, SourceUtil.backoffRetry(new RuntimeException("unavailable"), 3, config, 5));
    }

    @Test
    void backoffReturnsWhenTheSleepIsInterrupted() {
        CassandraSourceConnectorConfig config = config(new HashMap<>());
        Thread.currentThread().interrupt();
        try {
            long start = System.nanoTime();
            // A one hour cap: only the interrupt can end this sleep quickly.
            assertEquals(1, SourceUtil.backoffRetry(new RuntimeException("unavailable"), 0, config, 3_600_000L));
            assertTrue(System.nanoTime() - start < 60_000_000_000L);
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void givesUpOnAnUnreachableClusterAfterTheMaxBackoff() {
        Map<String, String> extra = new HashMap<>();
        // Port 1 is never a Cassandra node, so every connection attempt is refused straight away.
        extra.put(CassandraSourceConnectorConfig.CONTACT_POINTS_OPT, "127.0.0.1");
        extra.put(CassandraSourceConnectorConfig.PORT_OPT, "1");
        extra.put(CassandraSourceConnectorConfig.DC_OPT, "dc1");
        extra.put(CassandraSourceConnectorConfig.QUERY_MAX_BACKOFF_IN_SEC_CONFIG, "1");
        CassandraSourceConnectorConfig config = config(extra);

        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> SourceUtil.initCassandraClientWithRetry(config, "source-util-test", mock(SchemaChangeListener.class)));

        assertTrue(e.getMessage().contains("giving up"));
        assertNotNull(e.getCause());
    }
}
