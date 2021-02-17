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
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.google.common.collect.ImmutableList;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micronaut.configuration.cassandra.CassandraConfiguration;
import io.micronaut.configuration.cassandra.CassandraSessionFactory;
import io.vavr.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

//@Requires(configuration = "default")
@Singleton
public class CassandraService {
    private static final Logger logger = LoggerFactory.getLogger(CassandraService.class);

    final CassandraSessionFactory cassandraSessionFactory;
    final CassandraConfiguration cassandraConfiguration;
    final MeterRegistry meterRegistry;

    public CassandraService(CassandraSessionFactory cassandraSessionFactory,
                            CassandraConfiguration cassandraConfiguration,
                            MeterRegistry meterRegistry) {
        this.cassandraSessionFactory = cassandraSessionFactory;
        this.cassandraConfiguration = cassandraConfiguration;
        this.meterRegistry = meterRegistry;
    }

    public CassandraConfiguration getCassandraConfiguration() {
        return this.cassandraConfiguration;
    }

    public CompletionStage<CqlSession> getSession() {
        return cassandraSessionFactory
                .session(cassandraConfiguration)
                .buildAsync();
    }

    public Tuple2<String, ConsistencyLevel> selectRow(MutationKey pk, UUID nodeId, List<ConsistencyLevel> consistencyLevels) throws ExecutionException, InterruptedException {
        return selectRowAsync(pk, nodeId, consistencyLevels).toCompletableFuture().get();
    }

    /**
     * Try to read CL=ALL (could be LOCAL_ALL), retry LOCAL_QUORUM, retry LOCAL_ONE.
     * @param pk
     * @param nodeId
     * @return
     */
    public CompletionStage<Tuple2<String, ConsistencyLevel>> selectRowAsync(MutationKey pk, UUID nodeId, List<ConsistencyLevel> consistencyLevels) {
        final Iterable<Tag> tags = ImmutableList.of(Tag.of("keyspace", pk.getKeyspace()), Tag.of("table", pk.getTable()));
        return getSession()
                .thenComposeAsync(s -> {
                    Metadata metadata = s.getMetadata();
                    Optional<KeyspaceMetadata> keyspaceMetadataOptional = metadata.getKeyspace(pk.getKeyspace());
                    if (!keyspaceMetadataOptional.isPresent()) {
                        throw new IllegalArgumentException("No metadata for keyspace " + pk.getKeyspace());
                    }
                    Optional<TableMetadata> tableMetadataOptional = keyspaceMetadataOptional.get().getTable(pk.getTable());
                    if (!tableMetadataOptional.isPresent()) {
                        throw new IllegalArgumentException("No metadata for table " + pk.getKeyspace() + "." + pk.getTable());
                    }

                    Select query = QueryBuilder.selectFrom(pk.getKeyspace(), pk.getTable()).json().all();
                    for(ColumnMetadata cm : tableMetadataOptional.get().getPrimaryKey())
                        query = query.whereColumn(cm.getName()).isEqualTo(QueryBuilder.bindMarker());
                    SimpleStatement statement = query.build(pk.getPkColumns());

                    // set the coordinator node
                    if (nodeId != null) {
                        Node node = s.getMetadata().getNodes().get(nodeId);
                        if (node != null) {
                            logger.debug("node={} query={}", node.getHostId(), query.toString());
                            statement.setNode(node);
                        } else {
                            logger.warn("Cannot get row pk={} from node={}", pk, nodeId);
                            meterRegistry.counter("cassandraNodeUnavailable").increment();
                        }
                    } else {
                        logger.debug("node=any query={}", query.toString());
                    }

                    return executeWithDowngradeConsistencyRetry(s, query.build(pk.getPkColumns()), consistencyLevels)
                            .thenApply(tuple -> {
                                Row row = tuple._1().one();
                                if (row == null) {
                                    logger.debug("Row cl={} id={} does not exist any more", tuple._2, pk.id);
                                    return new Tuple2<>("", tuple._2);
                                }
                                String json = row.get(0, String.class);
                                logger.debug("Row cl={} coordinator={} id={} source={}",
                                        tuple._2, tuple._1.getExecutionInfo().getCoordinator().getHostId(), pk.id, json);
                                meterRegistry.counter("cassandraRead", tags).increment();
                                return new Tuple2<>(json, tuple._2);
                            })
                            .whenComplete((tuple, error) -> {
                                if (error != null) {
                                    logger.warn("Failed to retrieve row: {}", error);
                                    meterRegistry.counter("cassandraError", tags).increment();
                                }
                            });
                });
    }

    CompletionStage<Tuple2<AsyncResultSet, ConsistencyLevel>> executeWithDowngradeConsistencyRetry(CqlSession session, SimpleStatement statement, List<ConsistencyLevel> consistencyLevels) {
        final ConsistencyLevel cl = consistencyLevels.remove(0);
        statement.setConsistencyLevel(cl);
        logger.debug("Trying with CL={} statement={}", cl, statement);
        final CompletionStage<Tuple2<AsyncResultSet, ConsistencyLevel>> completionStage = session.executeAsync(statement).thenApply(rx -> new Tuple2<>(rx, cl));
        return completionStage
                .handle((r, ex) -> {
                    if (ex == null || !(ex instanceof UnavailableException) || consistencyLevels.isEmpty()) {
                        logger.debug("Executed CL={} statement={}", cl, statement);
                        return completionStage;
                    }
                    return completionStage
                            .handleAsync((r1, ex1) -> executeWithDowngradeConsistencyRetry(session, statement, consistencyLevels))
                            .thenCompose(Function.identity());
                })
                .thenCompose(Function.identity());
    }
}