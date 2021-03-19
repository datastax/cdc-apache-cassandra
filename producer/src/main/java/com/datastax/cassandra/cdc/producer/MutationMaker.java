/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.datastax.cassandra.cdc.producer;

import com.datastax.cassandra.cdc.producer.exceptions.CassandraConnectorTaskException;
import io.debezium.function.BlockingConsumer;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.MD5Digest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.time.Instant;
import java.util.UUID;

/**
 * Responsible for generating ChangeRecord and/or TombstoneRecord for create/update/delete events, as well as EOF events.
 */
@Singleton
public class MutationMaker {
    private static final Logger LOGGER = LoggerFactory.getLogger(MutationMaker.class);
    private final boolean emitTombstoneOnDelete;

    public MutationMaker(CassandraCdcConfiguration config) {
        this.emitTombstoneOnDelete = config.tombstonesOnDelete;
    }

    public void insert(String cluster, UUID node, CommitLogPosition offsetPosition,
                       String keyspace, String name, boolean snapshot,
                       Instant tsMicro, RowData data,
                       boolean markOffset, BlockingConsumer<Mutation> consumer,
                       String md5Digest, TableMetadata tableMetadata) {
        createRecord(cluster, node, offsetPosition, keyspace, name, snapshot, tsMicro,
                data, markOffset, consumer, md5Digest, tableMetadata);
    }

    public void update(String cluster, UUID node, CommitLogPosition offsetPosition,
                       String keyspace, String name, boolean snapshot,
                       Instant tsMicro, RowData data,
                       boolean markOffset, BlockingConsumer<Mutation> consumer,
                       String md5Digest, TableMetadata tableMetadata) {
        createRecord(cluster, node, offsetPosition, keyspace, name, snapshot, tsMicro,
                data, markOffset, consumer, md5Digest, tableMetadata);
    }

    public void delete(String cluster, UUID node, CommitLogPosition offsetPosition,
                       String keyspace, String name, boolean snapshot,
                       Instant tsMicro, RowData data,
                       boolean markOffset, BlockingConsumer<Mutation> consumer,
                       String md5Digest, TableMetadata tableMetadata) {
        createRecord(cluster, node, offsetPosition, keyspace, name, snapshot, tsMicro,
                data, markOffset, consumer, md5Digest, tableMetadata);
    }

    private void createRecord(String cluster, UUID node, CommitLogPosition offsetPosition,
                              String keyspace, String name, boolean snapshot,
                              Instant tsMicro, RowData data,
                              boolean markOffset, BlockingConsumer<Mutation> consumer,
                              String md5Digest, TableMetadata tableMetadata) {
        // TODO: filter columns
        RowData filteredData = data;

        SourceInfo source = new SourceInfo(cluster, node, offsetPosition, keyspace, name, tsMicro);
        Mutation record = new Mutation(offsetPosition.segmentId, offsetPosition.position, source, filteredData,
                markOffset, tsMicro.toEpochMilli(), md5Digest, tableMetadata);
        try {
            consumer.accept(record);
        }
        catch (InterruptedException e) {
            LOGGER.error("Interruption while enqueuing Change Event {}", record.toString());
            throw new CassandraConnectorTaskException("Enqueuing has been interrupted: ", e);
        }
    }

}
