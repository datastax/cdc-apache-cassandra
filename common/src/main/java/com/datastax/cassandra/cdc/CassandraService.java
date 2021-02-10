package com.datastax.cassandra.cdc;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.google.common.collect.ImmutableList;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micronaut.configuration.cassandra.CassandraConfiguration;
import io.micronaut.configuration.cassandra.CassandraSessionFactory;
import io.micronaut.context.annotation.Requires;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

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

    public String selectRow(MutationKey pk, UUID nodeId) throws ExecutionException, InterruptedException {
        return selectRowAsync(pk, nodeId).toCompletableFuture().get();
    }

    public CompletionStage<String> selectRowAsync(MutationKey pk, UUID nodeId) {
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
                    statement.setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
                    if (nodeId != null) {
                        Node node = s.getMetadata().getNodes().get(nodeId);
                        if (node != null) {
                            logger.debug("node={} query={}", node.getHostId(), query.toString());
                            statement.setNode(node);
                        } else {
                            logger.warn("Cannot get row pk={} from node={}", pk, nodeId);
                            meterRegistry.counter("cassandraNodeUnavailable").increment();
                            throw new IllegalStateException("Node=" + nodeId + " not available");
                        }
                    } else {
                        logger.debug("query={}", query.toString());
                    }
                    return s.executeAsync(statement)
                            .thenApply(rs -> {
                                Row row = rs.one();
                                if (row == null) {
                                    logger.debug("Row id={} does not exist any more", pk.id);
                                    return "";
                                }
                                String json = row.get(0, String.class);
                                logger.debug("Row id={} source={}", pk.id, json);
                                meterRegistry.counter("cassandraRead", tags).increment();
                                return json;
                            })
                            .whenComplete((map, error) -> {
                                if (error != null) {
                                    logger.warn("Failed to retrieve row: {}", error);
                                    meterRegistry.counter("cassandraError", tags).increment();
                                }
                            });
                });
    }

}