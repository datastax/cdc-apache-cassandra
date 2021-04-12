package com.datastax.cassandra.cdc;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.servererrors.UnavailableException;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import io.vavr.Tuple2;
import io.vavr.Tuple3;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;

/**
 * Async read from Cassandra with downgrade consistency retry.
 */
@Slf4j
@Getter
@SuppressWarnings("try")
public class CassandraClient implements AutoCloseable {

    final CqlSession cqlSession;

    public CassandraClient(CqlSession session) {
        this.cqlSession = session;
    }

    @Override
    public void close() throws Exception {
        this.cqlSession.close();
    }

    public Tuple3<Row, ConsistencyLevel, KeyspaceMetadata> selectRow(String keyspaceName,
                                                                     String tableName,
                                                                     List<String> pkColumns,
                                                                     Object[] pkValues,
                                                                     UUID nodeId,
                                                                     List<ConsistencyLevel> consistencyLevels)
            throws ExecutionException, InterruptedException {
        return selectRowAsync(keyspaceName, tableName, pkColumns, pkValues, nodeId, consistencyLevels)
                .toCompletableFuture().get();
    }

    public Tuple2<KeyspaceMetadata, TableMetadata> getTableMetadata(String keyspace, String table) {
        Metadata metadata = cqlSession.getMetadata();
        Optional<KeyspaceMetadata> keyspaceMetadataOptional = metadata.getKeyspace(keyspace);
        if(!keyspaceMetadataOptional.isPresent()) {
            throw new IllegalArgumentException("No metadata for keyspace " + keyspace);
        }
        Optional<TableMetadata> tableMetadataOptional = keyspaceMetadataOptional.get().getTable(table);
        if(!tableMetadataOptional.isPresent()) {
            throw new IllegalArgumentException("No metadata for table " + keyspace + "." + table);
        }
        return new Tuple2<>(keyspaceMetadataOptional.get(), tableMetadataOptional.get());
    }

    /**
     * Try to read CL=ALL (could be LOCAL_ALL), retry LOCAL_QUORUM, retry LOCAL_ONE.
     *
     */
    public CompletionStage<Tuple3<Row, ConsistencyLevel, KeyspaceMetadata>> selectRowAsync(String keyspaceName,
                                                                                           String tableName,
                                                                                           List<String> pkColumns,
                                                                                           Object[] pkValues,
                                                                                           UUID nodeId,
                                                                                           List<ConsistencyLevel> consistencyLevels) {
        TableMetadata tableMetadata = getTableMetadata(keyspaceName, tableName)._2;
        Select query = selectFrom(keyspaceName, tableName).all();
        for(String pkColumn : pkColumns)
            query = query.whereColumn(pkColumn).isEqualTo(bindMarker());
        SimpleStatement statement = query.build(pkValues);

        // set the coordinator node
        if(nodeId != null) {
            Node node = cqlSession.getMetadata().getNodes().get(nodeId);
            if(node != null) {
                log.debug("node={} query={}", node.getHostId(), query.toString());
                statement.setNode(node);
            } else {
                log.warn("Cannot get row {}}={} from node={}", pkColumns, Arrays.asList(pkValues), nodeId);
            }
        } else {
            log.debug("node=any query={}", query.toString());
        }

        return executeWithDowngradeConsistencyRetry(cqlSession, keyspaceName, query.build(pkColumns), consistencyLevels)
                .thenApply(tuple -> {
                    log.debug("Read cl={} coordinator={} pk={}",
                            tuple._2, tuple._1.getExecutionInfo().getCoordinator().getHostId(), Arrays.asList(pkColumns));
                    KeyspaceMetadata keyspaceMetadata = cqlSession.getMetadata().getKeyspace(keyspaceName).get();
                    Row row = tuple._1.one();
                    return new Tuple3<>(
                            row,
                            tuple._2,
                            keyspaceMetadata);
                })
                .whenComplete((tuple, error) -> {
                    if(error != null) {
                        log.warn("Failed to retrieve row: {}", error); }
                });
    }

    CompletionStage<Tuple2<AsyncResultSet, ConsistencyLevel>> executeWithDowngradeConsistencyRetry(
            CqlSession cqlSession,
            String keyspaceName,
            SimpleStatement statement,
            List<ConsistencyLevel> consistencyLevels) {
        final ConsistencyLevel cl = consistencyLevels.remove(0);
        statement.setConsistencyLevel(cl);
        log.debug("Trying with CL={} statement={}", cl, statement);
        final CompletionStage<Tuple2<AsyncResultSet, ConsistencyLevel>> completionStage =
                cqlSession.executeAsync(statement).thenApply(rx -> new Tuple2<>(rx, cl));
        return completionStage
                .handle((r, ex) -> {
                    if(ex == null || !(ex instanceof UnavailableException) || consistencyLevels.isEmpty()) {
                        log.debug("Executed CL={} statement={}", cl, statement);
                        return completionStage;
                    }
                    return completionStage
                            .handleAsync((r1, ex1) ->
                                    executeWithDowngradeConsistencyRetry(cqlSession, keyspaceName, statement, consistencyLevels))
                            .thenCompose(Function.identity());
                })
                .thenCompose(Function.identity());
    }
}
