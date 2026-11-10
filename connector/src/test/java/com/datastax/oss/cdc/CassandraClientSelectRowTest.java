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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import io.vavr.Tuple2;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Covers {@link CassandraClient#selectRow} and {@link CassandraClient#prepareSelect} against a mocked
 * driver session. The client is created without its constructor, which would connect to a cluster.
 */
class CassandraClientSelectRowTest {

    private static final Logger CLIENT_LOG = (Logger) LoggerFactory.getLogger(CassandraClient.class);
    private static Level previousLevel;

    private CassandraClient client;
    private CqlSession session;
    private PreparedStatement prepared;
    private BoundStatement bound;
    private final UUID nodeId = UUID.randomUUID();
    private Node node;

    /** Runs at DEBUG so the debug-only log statements are exercised too. */
    @BeforeAll
    static void enableDebugLogging() {
        previousLevel = CLIENT_LOG.getLevel();
        CLIENT_LOG.setLevel(Level.DEBUG);
    }

    @AfterAll
    static void restoreLogging() {
        CLIENT_LOG.setLevel(previousLevel);
    }

    @BeforeEach
    void setUp() throws Exception {
        session = mock(CqlSession.class);
        client = mock(CassandraClient.class, Mockito.CALLS_REAL_METHODS);
        Field f = CassandraClient.class.getDeclaredField("cqlSession");
        f.setAccessible(true);
        f.set(client, session);

        bound = mock(BoundStatement.class);
        // bind(Object...) is varargs, so answer by method name rather than with an argument matcher.
        prepared = mock(PreparedStatement.class, invocation -> "bind".equals(invocation.getMethod().getName())
                ? bound : Mockito.RETURNS_DEFAULTS.answer(invocation));
        when(bound.setConsistencyLevel(any())).thenReturn(bound);
        when(bound.setNode(any())).thenReturn(bound);

        node = mock(Node.class);
        Metadata metadata = mock(Metadata.class);
        when(metadata.getNodes()).thenReturn(Collections.singletonMap(nodeId, node));
        when(session.getMetadata()).thenReturn(metadata);
    }

    private AsyncResultSet resultSet(Row row, UUID coordinatorId) {
        AsyncResultSet rs = mock(AsyncResultSet.class);
        ExecutionInfo info = mock(ExecutionInfo.class);
        when(rs.getExecutionInfo()).thenReturn(info);
        if (coordinatorId != null) {
            Node coordinator = mock(Node.class);
            when(coordinator.getHostId()).thenReturn(coordinatorId);
            when(info.getCoordinator()).thenReturn(coordinator);
        }
        when(rs.one()).thenReturn(row);
        return rs;
    }

    @Test
    void readsTheRowFromTheMutationCoordinatorWhenItIsUp() throws Exception {
        when(node.getState()).thenReturn(NodeState.UP);
        Row row = mock(Row.class);
        AsyncResultSet rs = resultSet(row, nodeId);
        when(session.executeAsync(bound)).thenReturn(CompletableFuture.completedFuture(rs));

        Tuple2<Row, UUID> result = client.selectRow(Arrays.asList("a", 1), nodeId,
                ConsistencyLevel.LOCAL_QUORUM, prepared, "md5");

        assertSame(row, result._1);
        assertEquals(nodeId, result._2);
        verify(bound).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        verify(bound).setNode(node);
    }

    @Test
    void doesNotPinADownCoordinatorAndHandlesAMissingCoordinatorId() throws Exception {
        when(node.getState()).thenReturn(NodeState.DOWN);
        AsyncResultSet rs = resultSet(null, null);
        when(session.executeAsync(bound)).thenReturn(CompletableFuture.completedFuture(rs));

        Tuple2<Row, UUID> result = client.selectRow(Collections.singletonList("a"), nodeId,
                ConsistencyLevel.ONE, prepared, "md5");

        assertNull(result._1);
        assertNull(result._2);
        verify(bound, never()).setNode(any());
    }

    @Test
    void propagatesAFailedRead() {
        CompletableFuture<AsyncResultSet> failed = new CompletableFuture<>();
        failed.completeExceptionally(new IllegalStateException("read timeout"));
        when(session.executeAsync(bound)).thenReturn(failed);

        ExecutionException e = assertThrows(ExecutionException.class, () -> client.selectRow(
                Collections.singletonList("a"), null, ConsistencyLevel.ONE, prepared, "md5"));

        assertTrue(e.getCause() instanceof IllegalStateException);
        verify(session, never()).getMetadata();
    }

    @Test
    void preparesASelectOnTheGivenPrimaryKeyPrefix() {
        PreparedStatement statement = mock(PreparedStatement.class);
        when(session.prepare(anyString())).thenReturn(statement);
        CqlIdentifier[] pk = {CqlIdentifier.fromInternal("a"), CqlIdentifier.fromInternal("b")};

        assertSame(statement, client.prepareSelect("ks", "t", new CqlIdentifier[0], pk, 1));

        verify(session).prepare(Mockito.startsWith("SELECT a,b FROM ks.t WHERE a=?"));
    }
}
