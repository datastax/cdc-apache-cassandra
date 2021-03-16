package com.datastax.oss.pulsar.source;

import com.datastax.cassandra.cdc.MutationKey;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
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
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.google.common.collect.ImmutableList;
import groovy.lang.Singleton;
import io.vavr.Tuple2;
import io.vavr.Tuple3;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.pulsar.client.api.schema.FieldSchemaBuilder;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.common.schema.SchemaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

/**
 * Async read from Cassandra with downgrade consistency retry.
 */
@Slf4j
public class CassandraClient implements AutoCloseable {

    final CqlSession cqlSession;

    public CassandraClient(CqlSession session) {
        this.cqlSession = session;
    }

    public void close() throws Exception {
        this.cqlSession.close();
    }

    public Tuple3<Row, ConsistencyLevel, KeyspaceMetadata> selectRow(MutationKey pk, UUID nodeId, List<ConsistencyLevel> consistencyLevels) throws ExecutionException, InterruptedException {
        return selectRowAsync(pk, nodeId, consistencyLevels).toCompletableFuture().get();
    }

    /**
     * Try to read CL=ALL (could be LOCAL_ALL), retry LOCAL_QUORUM, retry LOCAL_ONE.
     *
     * @param pk
     * @param nodeId
     * @return
     */
    public CompletionStage<Tuple3<Row, ConsistencyLevel, KeyspaceMetadata>> selectRowAsync(MutationKey pk, UUID nodeId, List<ConsistencyLevel> consistencyLevels) {
        Metadata metadata = cqlSession.getMetadata();
        Optional<KeyspaceMetadata> keyspaceMetadataOptional = metadata.getKeyspace(pk.getKeyspace());
        if(!keyspaceMetadataOptional.isPresent()) {
            throw new IllegalArgumentException("No metadata for keyspace " + pk.getKeyspace());
        }
        Optional<TableMetadata> tableMetadataOptional = keyspaceMetadataOptional.get().getTable(pk.getTable());
        if(!tableMetadataOptional.isPresent()) {
            throw new IllegalArgumentException("No metadata for table " + pk.getKeyspace() + "." + pk.getTable());
        }

        Select query = selectFrom(pk.getKeyspace(), pk.getTable()).json().all();
        for(ColumnMetadata cm : tableMetadataOptional.get().getPrimaryKey())
            query = query.whereColumn(cm.getName()).isEqualTo(QueryBuilder.bindMarker());
        SimpleStatement statement = query.build(pk.getPkColumns());

        // set the coordinator node
        if(nodeId != null) {
            Node node = cqlSession.getMetadata().getNodes().get(nodeId);
            if(node != null) {
                log.debug("node={} query={}", node.getHostId(), query.toString());
                statement.setNode(node);
            } else {
                log.warn("Cannot get row pk={} from node={}", pk, nodeId);
            }
        } else {
            log.debug("node=any query={}", query.toString());
        }

        return executeWithDowngradeConsistencyRetry(cqlSession, query.build(pk.getPkColumns()), consistencyLevels)
                .thenApply(tuple -> {
                    log.debug("Read cl={} coordinator={} pk={}",
                            tuple._2, tuple._1.getExecutionInfo().getCoordinator().getHostId(), pk, tuple._1);
                    KeyspaceMetadata keyspaceMetadata = cqlSession.getMetadata().getKeyspace(pk.getKeyspace()).get();
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
                                    executeWithDowngradeConsistencyRetry(cqlSession, statement, consistencyLevels))
                            .thenCompose(Function.identity());
                })
                .thenCompose(Function.identity());
    }

    public RecordSchemaBuilder buildSchema(KeyspaceMetadata ksm, String tableName) {
        if(!ksm.getTable(tableName).isPresent())
            throw new IllegalArgumentException("Table metadata not found");

        TableMetadata tm = ksm.getTable(tableName).get();
        RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record(ksm.getName().toString() + "." + tableName);
        List<CqlIdentifier> pkNames = tm.getPrimaryKey().stream().map(ColumnMetadata::getName).collect(Collectors.toList());
        for(ColumnMetadata cm : tm.getColumns().values()) {
            FieldSchemaBuilder<?> fieldSchemaBuilder = null;
            switch(cm.getType().getProtocolCode()) {
                case ProtocolConstants.DataType.ASCII:
                case ProtocolConstants.DataType.VARCHAR:
                    fieldSchemaBuilder = recordSchemaBuilder.field(cm.getName().toString()).type(SchemaType.STRING);
                    break;
                case ProtocolConstants.DataType.INT:
                    fieldSchemaBuilder = recordSchemaBuilder.field(cm.getName().toString()).type(SchemaType.INT32);
                    break;
                case ProtocolConstants.DataType.BIGINT:
                    fieldSchemaBuilder = recordSchemaBuilder.field(cm.getName().toString()).type(SchemaType.INT64);
                    break;
                case ProtocolConstants.DataType.BOOLEAN:
                    fieldSchemaBuilder = recordSchemaBuilder.field(cm.getName().toString()).type(SchemaType.BOOLEAN);
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported DataType=" + cm.getType().getProtocolCode());
            }
            if(pkNames.contains(cm.getName())) {
                int position = pkNames.indexOf(cm.getName());
                fieldSchemaBuilder.required().property("primary_key_order", Integer.toString(position));
            } else {
                fieldSchemaBuilder.optional();
            }
        }
        return recordSchemaBuilder;
    }

    public Schema buildAvroSchema(KeyspaceMetadata ksm, String tableName) {
        if(!ksm.getTable(tableName).isPresent())
            throw new IllegalArgumentException("Table not found");

        TableMetadata tm = ksm.getTable(tableName).get();
        org.apache.avro.SchemaBuilder.RecordBuilder<Schema> recordBuilder = org.apache.avro.SchemaBuilder
                .record(ksm.getName().toString() + "." + tableName)
                .namespace("default");
        org.apache.avro.SchemaBuilder.FieldAssembler<Schema> fieldAssembler = recordBuilder.fields();
        for(ColumnMetadata cm : tm.getColumns().values()) {
            boolean pk = false;
            for(ColumnMetadata cm2 : tm.getPrimaryKey()) {
                if(cm2.getName().equals(cm.getName())) {
                    pk = true;
                    break;
                }
            }
            switch(cm.getType().getProtocolCode()) {
                case ProtocolConstants.DataType.ASCII:
                case ProtocolConstants.DataType.VARCHAR:
                    if(pk) {
                        fieldAssembler.requiredString(cm.getName().toString());
                    } else {
                        fieldAssembler.optionalString(cm.getName().toString());
                    }
                    break;
                case ProtocolConstants.DataType.INT:
                    if(pk) {
                        fieldAssembler.requiredInt(cm.getName().toString());
                    } else {
                        fieldAssembler.optionalInt(cm.getName().toString());
                    }
                    break;
                case ProtocolConstants.DataType.BIGINT:
                    if(pk) {
                        fieldAssembler.requiredLong(cm.getName().toString());
                    } else {
                        fieldAssembler.optionalLong(cm.getName().toString());
                    }
                    break;
                case ProtocolConstants.DataType.BOOLEAN:
                    if(pk) {
                        fieldAssembler.requiredBoolean(cm.getName().toString());
                    } else {
                        fieldAssembler.optionalBoolean(cm.getName().toString());
                    }
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported DataType=" + cm.getType().getProtocolCode());
            }
        }
        return fieldAssembler.endRecord();
    }
}