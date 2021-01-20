package com.datastax.cassandra.cdc.consumer;

import com.datastax.cassandra.cdc.EventKey;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import io.micronaut.configuration.cassandra.CassandraConfiguration;
import io.micronaut.configuration.cassandra.CassandraSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;

@Singleton
public class CassandraService {
    private static final Logger logger = LoggerFactory.getLogger(CassandraService.class);

    final CassandraSessionFactory cassandraSessionFactory;
    final CassandraConfiguration cassandraConfiguration;

    public CassandraService(CassandraSessionFactory cassandraSessionFactory,
                            CassandraConfiguration cassandraConfiguration) {
        this.cassandraSessionFactory = cassandraSessionFactory;
        this.cassandraConfiguration = cassandraConfiguration;
    }

    public CompletionStage<CqlSession> getSession() {
        return cassandraSessionFactory.session(cassandraConfiguration).buildAsync();
    }

    public CompletionStage<String> selectRowAsync(EventKey pk, UUID nodeId) {
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
                    Select query = selectFrom(pk.getKeyspace(), pk.getTable()).json().all();
                    for(ColumnMetadata cm : tableMetadataOptional.get().getPrimaryKey())
                        query = query.whereColumn(cm.getName()).isEqualTo(bindMarker());
                    SimpleStatement statement = query.build(pk.getPkColumns());
                    statement.setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
                    if (nodeId != null) {
                        Node node = s.getMetadata().getNodes().get(nodeId);
                        if (node != null) {
                            logger.debug("node={} query={}", node.getHostId(), query.toString());
                            statement.setNode(node);
                        }
                    } else {
                        logger.debug("query={}", query.toString());
                    }
                    return s.executeAsync(statement);
                })
                .thenApply(rs -> {
                    Row row = rs.one();
                    String json = row.get(0, String.class);
                    logger.debug("Row id={} source={}", pk.id(), json);
                    return json;
                })
                .whenComplete((map, error) -> {
                    if (error != null) {
                        logger.warn("Failed to retrieve row: {}", error);
                    }
                });
    }

}
