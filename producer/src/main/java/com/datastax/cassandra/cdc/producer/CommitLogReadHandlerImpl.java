/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.datastax.cassandra.cdc.producer;

import com.datastax.cassandra.cdc.*;
import com.datastax.cassandra.cdc.producer.exceptions.CassandraConnectorSchemaException;
import com.datastax.cassandra.cdc.producer.exceptions.CassandraConnectorTaskException;
import io.debezium.DebeziumException;
import io.debezium.time.Conversions;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.commitlog.CommitLogDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.CommitLogReadHandler;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.common.schema.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Singleton;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static com.datastax.cassandra.cdc.producer.CommitLogReadHandlerImpl.RowType.DELETE;

/**
 * Handler that implements {@link CommitLogReadHandler} interface provided by Cassandra source code.
 *
 * This handler implementation processes each {@link org.apache.cassandra.db.Mutation} and invokes one of the registered partition handler
 * for each {@link PartitionUpdate} in the {@link org.apache.cassandra.db.Mutation} (a mutation could have multiple partitions if it is a batch update),
 * which in turn makes one or more record via the {@link MutationMaker}.
 */
@Singleton
public class CommitLogReadHandlerImpl implements CommitLogReadHandler {
    private static final Logger logger = LoggerFactory.getLogger(CommitLogReadHandlerImpl.class);

    private static final boolean MARK_OFFSET = true;

    private final MutationMaker recordMaker;
    private final OffsetFileWriter offsetWriter;
    private final MeterRegistry meterRegistry;
    private final CassandraCdcConfiguration config;
    private final CassandraService cassandraService;
    private final MutationSender<KeyValue<MutationKey, MutationValue>> mutationSender;

    CommitLogReadHandlerImpl(CassandraCdcConfiguration config,
                             OffsetFileWriter offsetFileWriter,
                             MutationMaker recordMaker,
                             @Nullable CassandraService cassandraService,
                             MutationSender<KeyValue<MutationKey, MutationValue>> mutationSender,
                             MeterRegistry meterRegistry) {
        this.config = config;
        this.cassandraService = cassandraService;
        this.mutationSender = mutationSender;

        this.offsetWriter = offsetFileWriter;
        this.recordMaker = recordMaker;
        this.meterRegistry = meterRegistry;
    }

    /**
     *  A PartitionType represents the type of a PartitionUpdate.
     */
    enum PartitionType {
        /**
         * a partition-level deletion where partition key = primary key (no clustering key)
         */
        PARTITION_KEY_ROW_DELETION,

        /**
         *  a partition-level deletion where partition key + clustering key = primary key
         */
        PARTITION_AND_CLUSTERING_KEY_ROW_DELETION,

        /**
         * a row-level modification
         */
        ROW_LEVEL_MODIFICATION,

        /**
         * an update on materialized view
         */
        MATERIALIZED_VIEW,

        /**
         * an update on secondary index
         */
        SECONDARY_INDEX,

        /**
         * an update on a table that contains counter data type
         */
        COUNTER;

        static final Set<PartitionType> supportedPartitionTypes = new HashSet<>(Arrays.asList(PARTITION_KEY_ROW_DELETION, ROW_LEVEL_MODIFICATION));

        public static PartitionType getPartitionType(PartitionUpdate pu) {
            if (pu.metadata().isCounter()) {
                return COUNTER;
            }
            else if (pu.metadata().isView()) {
                return MATERIALIZED_VIEW;
            }
            else if (pu.metadata().isIndex()) {
                return SECONDARY_INDEX;
            }
            else if (isPartitionDeletion(pu) && hasClusteringKeys(pu)) {
                return PARTITION_AND_CLUSTERING_KEY_ROW_DELETION;
            }
            else if (isPartitionDeletion(pu) && !hasClusteringKeys(pu)) {
                return PARTITION_KEY_ROW_DELETION;
            }
            else {
                return ROW_LEVEL_MODIFICATION;
            }
        }

        public static boolean isValid(PartitionType type) {
            return supportedPartitionTypes.contains(type);
        }

        public static boolean hasClusteringKeys(PartitionUpdate pu) {
            return !pu.metadata().clusteringColumns().isEmpty();
        }

        public static boolean isPartitionDeletion(PartitionUpdate pu) {
            return pu.partitionLevelDeletion().markedForDeleteAt() > LivenessInfo.NO_TIMESTAMP;
        }
    }

    /**
     *  A RowType represents different types of {@link Row}-level modifications in a Cassandra table.
     */
    enum RowType {
        /**
         * Single-row insert
         */
        INSERT,

        /**
         * Single-row update
         */
        UPDATE,

        /**
         * Single-row delete
         */
        DELETE,

        /**
         * A row-level deletion that deletes a range of keys.
         * For example: DELETE * FROM table WHERE partition_key = 1 AND clustering_key > 0;
         */
        RANGE_TOMBSTONE,

        /**
         * Unknown row-level operation
         */
        UNKNOWN;

        static final Set<RowType> supportedRowTypes = new HashSet<>(Arrays.asList(INSERT, UPDATE, DELETE));

        public static RowType getRowType(Unfiltered unfiltered) {
            if (unfiltered.isRangeTombstoneMarker()) {
                return RANGE_TOMBSTONE;
            }
            else if (unfiltered.isRow()) {
                Row row = (Row) unfiltered;
                if (isDelete(row)) {
                    return DELETE;
                }
                else if (isInsert(row)) {
                    return INSERT;
                }
                else if (isUpdate(row)) {
                    return UPDATE;
                }
            }
            return UNKNOWN;
        }

        public static boolean isValid(RowType rowType) {
            return supportedRowTypes.contains(rowType);
        }

        public static boolean isDelete(Row row) {
            return row.deletion().time().markedForDeleteAt() > LivenessInfo.NO_TIMESTAMP;
        }

        public static boolean isInsert(Row row) {
            return row.primaryKeyLivenessInfo().timestamp() > LivenessInfo.NO_TIMESTAMP;
        }

        public static boolean isUpdate(Row row) {
            return row.primaryKeyLivenessInfo().timestamp() == LivenessInfo.NO_TIMESTAMP;
        }
    }

    @Override
    public void handleMutation(org.apache.cassandra.db.Mutation mutation, int size, int entryLocation, CommitLogDescriptor descriptor) {
        if (mutation.getKeyspaceName().equalsIgnoreCase(SchemaConstants.SCHEMA_KEYSPACE_NAME)) {
            logger.info("Schema update tables={}", mutation.getTableIds());
        }

        if (!mutation.trackedByCDC()) {
            return;
        }

        for (PartitionUpdate pu : mutation.getPartitionUpdates()) {
            CommitLogPosition entryPosition = new CommitLogPosition(CommitLogUtil.extractTimestamp(descriptor.fileName()), entryLocation);

            if (offsetWriter.offset().compareTo(entryPosition) > 0) {
                logger.debug("Mutation at {} for table {}.{} already processed, skipping...",
                        entryPosition, pu.metadata().keyspace, pu.metadata().name);
                return;
            }

            try {
                process(pu, entryPosition);
            }
            catch (Exception e) {
                throw new DebeziumException(String.format("Failed to process PartitionUpdate %s at %s for table %s.%s.",
                        pu.toString(), entryPosition.toString(), pu.metadata().keyspace, pu.metadata().name), e);
            }
        }
    }

    @Override
    public void handleUnrecoverableError(CommitLogReadException exception) throws IOException {
        logger.error("Unrecoverable error when reading commit log", exception);
        meterRegistry.counter("errors").increment();
    }

    @Override
    public boolean shouldSkipSegmentOnError(CommitLogReadException exception) throws IOException {
        if (exception.permissible) {
            logger.error("Encountered a permissible exception during log replay", exception);
        }
        else {
            logger.error("Encountered a non-permissible exception during log replay", exception);
        }
        return false;
    }

    /**
     * Method which processes a partition update if it's valid (either a single-row partition-level
     * deletion or a row-level modification) or throw an exception if it isn't. The valid partition
     * update is then converted into a {@link Mutation}.
     */
    private void process(PartitionUpdate pu, CommitLogPosition position) {
        PartitionType partitionType = PartitionType.getPartitionType(pu);

        if (!PartitionType.isValid(partitionType)) {
            logger.warn("Encountered an unsupported partition type {}, skipping...", partitionType);
            return;
        }

        switch (partitionType) {
            case PARTITION_KEY_ROW_DELETION:
                handlePartitionDeletion(pu, position);
                break;

            case ROW_LEVEL_MODIFICATION:
                UnfilteredRowIterator it = pu.unfilteredIterator();
                while (it.hasNext()) {
                    Unfiltered rowOrRangeTombstone = it.next();
                    RowType rowType = RowType.getRowType(rowOrRangeTombstone);
                    if (!RowType.isValid(rowType)) {
                        logger.warn("Encountered an unsupported row type {}, skipping...", rowType);
                        continue;
                    }
                    Row row = (Row) rowOrRangeTombstone;

                    handleRowModifications(row, rowType, pu, position);
                }
                break;

            default:
                throw new CassandraConnectorSchemaException("Unsupported partition type " + partitionType + " should have been skipped");
        }
    }

    /**
     * Handle a valid deletion event resulted from a partition-level deletion by converting Cassandra representation
     * of this event into a {@link Mutation} object and send it to pulsar. A valid deletion
     * event means a partition only has a single row, this implies there are no clustering keys.
     *
     * The steps are:
     *      (1) Populate the "source" field for this event
     *      (3) Populate the "after" field for this event
     *          a. populate partition columns
     *          b. populate regular columns with null values
     *      (4) Assemble a {@link Mutation} object from the populated data and queue the record
     */
    private void handlePartitionDeletion(PartitionUpdate pu, CommitLogPosition offsetPosition) {
        try {

            RowData after = new RowData();

            populatePartitionColumns(after, pu);

            /*
            // For partition deletions, the PartitionUpdate only specifies the partition key, it does not
            // contains any info on regular (non-partition) columns, as if they were not modified. In order
            // to differentiate deleted columns from unmodified columns, we populate the deleted columns
            // with null value and timestamps
            TableMetadata tableMetadata = keyValueSchema.tableMetadata();
            List<ColumnMetadata> clusteringColumns = tableMetadata.getClusteringColumns();
            if (!clusteringColumns.isEmpty()) {
                throw new CassandraConnectorSchemaException("Uh-oh... clustering key should not exist for partition deletion");
            }
            for (ColumnMetadata cm : tableMetadata.columns()) {
                if (!cm.isPrimaryKeyColumn()) {
                    String name = cm.name.toString();
                    long deletionTs = pu.deletionInfo().getPartitionDeletion().markedForDeleteAt();
                    CellData cellData = new CellData(name, null, deletionTs, CellData.ColumnType.REGULAR);
                    after.addCell(cellData);
                }
            }
            */

            recordMaker.delete(DatabaseDescriptor.getClusterName(), config.nodeId, offsetPosition,
                    pu.metadata().keyspace, pu.metadata().name, false,
                    Conversions.toInstantFromMicros(pu.maxTimestamp()), after,
                    MARK_OFFSET, this::blockingSend);
        }
        catch (Exception e) {
            logger.error("Fail to send delete partition at {}. Reason: {}", offsetPosition, e);
        }
    }

    /**
     * Handle a valid event resulted from a row-level modification by converting Cassandra representation of
     * this event into a {@link Mutation} object and sent it to pulsar. A valid event
     * implies this must be an insert, update, or delete.
     *
     * The steps are:
     *      (1) Populate the "source" field for this event
     *      (3) Populate the "after" field for this event
     *          a. populate partition columns
     *          b. populate clustering columns
     *          c. populate regular columns
     *          d. for deletions, populate regular columns with null values
     *      (4) Assemble a {@link Mutation} object from the populated data and queue the record
     */
    private void handleRowModifications(Row row, RowType rowType, PartitionUpdate pu, CommitLogPosition offsetPosition) {

        RowData after = new RowData();
        populatePartitionColumns(after, pu);
        populateClusteringColumns(after, row, pu);
        //populateRegularColumns(after, row, rowType);

        long ts = rowType == DELETE ? row.deletion().time().markedForDeleteAt() : pu.maxTimestamp();

        switch (rowType) {
            case INSERT:
                recordMaker.insert(DatabaseDescriptor.getClusterName(), config.nodeId, offsetPosition,
                        pu.metadata().keyspace, pu.metadata().name, false,
                        Conversions.toInstantFromMicros(ts), after, MARK_OFFSET, this::blockingSend);
                break;

            case UPDATE:
                recordMaker.update(DatabaseDescriptor.getClusterName(), config.nodeId, offsetPosition,
                        pu.metadata().keyspace, pu.metadata().name, false,
                        Conversions.toInstantFromMicros(ts), after, MARK_OFFSET, this::blockingSend);
                break;

            case DELETE:
                recordMaker.delete(DatabaseDescriptor.getClusterName(), config.nodeId, offsetPosition,
                        pu.metadata().keyspace, pu.metadata().name, false,
                        Conversions.toInstantFromMicros(ts), after, MARK_OFFSET, this::blockingSend);
                break;

            default:
                throw new CassandraConnectorTaskException("Unsupported row type " + rowType + " should have been skipped");
        }
    }

    private void populatePartitionColumns(RowData after, PartitionUpdate pu) {
        List<Object> partitionKeys = getPartitionKeys(pu);
        for (ColumnMetadata cd : pu.metadata().partitionKeyColumns()) {
            try {
                String name = cd.name.toString();
                Object value = partitionKeys.get(cd.position());
                CellData cellData = new CellData(name, value, null, CellData.ColumnType.PARTITION);
                after.addCell(cellData);
            }
            catch (Exception e) {
                throw new DebeziumException(String.format("Failed to populate Column %s with Type %s of Table %s in KeySpace %s.",
                        cd.name.toString(), cd.type.toString(), cd.cfName, cd.ksName), e);
            }
        }
    }

    private void populateClusteringColumns(RowData after, Row row, PartitionUpdate pu) {
        for (ColumnMetadata cd : pu.metadata().clusteringColumns()) {
            try {
                String name = cd.name.toString();
                Object value = row.clustering().get(cd.position());
                CellData cellData = new CellData(name, value, null, CellData.ColumnType.CLUSTERING);
                after.addCell(cellData);
            }
            catch (Exception e) {
                throw new DebeziumException(String.format("Failed to populate Column %s with Type %s of Table %s in KeySpace %s.",
                        cd.name.toString(), cd.type.toString(), cd.cfName, cd.ksName), e);
            }
        }
    }

    /*
    private void populateRegularColumns(RowData after, Row row, RowType rowType) {
        if (rowType == INSERT || rowType == UPDATE) {
            for (ColumnMetadata cd : row.columns()) {
                try {
                    Object value;
                    Object deletionTs = null;
                    AbstractType abstractType = cd.type;
                    if (abstractType.isCollection() && abstractType.isMultiCell()) {
                        ComplexColumnData ccd = row.getComplexColumnData(cd);
                        value = CassandraTypeDeserializer.deserialize((CollectionType) abstractType, ccd);
                    }
                    else {
                        org.apache.cassandra.db.rows.Cell cell = row.getCell(cd);
                        value = cell.isTombstone() ? null : CassandraTypeDeserializer.deserialize(abstractType, cell.value());
                        deletionTs = cell.isExpiring() ? TimeUnit.MICROSECONDS.convert(cell.localDeletionTime(), TimeUnit.SECONDS) : null;
                    }
                    String name = cd.name.toString();
                    CellData cellData = new CellData(name, value, deletionTs, CellData.ColumnType.REGULAR);
                    after.addCell(cellData);
                }
                catch (Exception e) {
                    throw new DebeziumException(String.format("Failed to populate Column %s with Type %s of Table %s in KeySpace %s.",
                            cd.name.toString(), cd.type.toString(), cd.cfName, cd.ksName), e);
                }
            }
        }
        else if (rowType == DELETE) {
            // For row-level deletions, row.columns() will result in an empty list and does not contain
            // the column definitions for the deleted columns. In order to differentiate deleted columns from
            // unmodified columns, we populate the deleted columns with null value and timestamps.
            TableMetadata tableMetadata = schema.tableMetadata();
            long deletionTs = row.deletion().time().markedForDeleteAt();
            for (ColumnMetadata cm : tableMetadata.columns()) {
                if (!cm.isPrimaryKeyColumn()) {
                    String name = cm.name.toString();
                    CellData cellData = new CellData(name, null, deletionTs, CellData.ColumnType.REGULAR);
                    after.addCell(cellData);
                }
            }
        }
    }
    */

    /**
     * Given a PartitionUpdate, deserialize the partition key byte buffer
     * into a list of partition key values.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    private static List<Object> getPartitionKeys(PartitionUpdate pu) {
        List<Object> values = new ArrayList<>();

        List<ColumnMetadata> columnDefinitions = pu.metadata().partitionKeyColumns();

        // simple partition key
        if (columnDefinitions.size() == 1) {
            ByteBuffer bb = pu.partitionKey().getKey();
            ColumnSpecification cs = columnDefinitions.get(0);
            AbstractType<?> type = cs.type;
            try {
                Object value = type.compose(bb);
                values.add(value);
            }
            catch (Exception e) {
                throw new DebeziumException(String.format("Failed to deserialize Column %s with Type %s in Table %s and KeySpace %s.",
                        cs.name.toString(), cs.type.toString(), cs.cfName, cs.ksName), e);
            }

            // composite partition key
        }
        else {
            ByteBuffer keyBytes = pu.partitionKey().getKey().duplicate();

            // 0xFFFF is reserved to encode "static column", skip if it exists at the start
            if (keyBytes.remaining() >= 2) {
                int header = ByteBufferUtil.getShortLength(keyBytes, keyBytes.position());
                if ((header & 0xFFFF) == 0xFFFF) {
                    ByteBufferUtil.readShortLength(keyBytes);
                }
            }

            // the encoding of columns in the partition key byte buffer is
            // <col><col><col>...
            // where <col> is:
            // <length of value><value><end-of-component byte>
            // <length of value> is a 2 bytes unsigned short (excluding 0xFFFF used to encode "static columns")
            // <end-of-component byte> should always be 0 for columns (1 for query bounds)
            // this section reads the bytes for each column and deserialize into objects based on each column type
            int i = 0;
            while (keyBytes.remaining() > 0 && i < columnDefinitions.size()) {
                ColumnSpecification cs = columnDefinitions.get(i);
                AbstractType<?> type = cs.type;
                ByteBuffer bb = ByteBufferUtil.readBytesWithShortLength(keyBytes);
                try {
                    Object value = type.compose(bb);
                    values.add(value);
                }
                catch (Exception e) {
                    throw new DebeziumException(String.format("Failed to deserialize Column %s with Type %s in Table %s and KeySpace %s",
                            cs.name.toString(), cs.type.toString(), cs.cfName, cs.ksName), e);
                }
                byte b = keyBytes.get();
                if (b != 0) {
                    break;
                }
                ++i;
            }
        }

        return values;
    }

    public void blockingSend(Mutation mutation) {
        CommitLogPosition sentOffset = this.mutationSender.sentOffset();
        long seg = sentOffset.segmentId;
        int pos = sentOffset.position;

        assert mutation != null : "Unexpected null mutation";
        assert mutation.getSegment() >= seg : "Unexpected mutation segment";
        assert mutation.getSegment() > seg ||
                (mutation.getSegment() == seg && mutation.getPosition() > pos)
                : "Unexpected mutation offset";

        logger.debug("Sending mutation={}", mutation);

        while(true) {
            try {
                processMutation(mutation).toCompletableFuture().get();
                break;
            } catch(Exception e) {
                logger.error("failed to send message to pulsar:", e);
                try {
                    Thread.sleep(10000);
                } catch(InterruptedException interruptedException) {
                }
            }
        }
    }

    CompletionStage<Void> processMutation(final Mutation mutation) throws PulsarClientException {
        if (cassandraService != null && !Operation.DELETE.equals(mutation.getOp())) {
            // read the cassandra row from the local node, for insert/update operation
            return cassandraService.selectRowAsync(mutation.mutationKey(), mutation.getSource().getNodeId())
                    .thenComposeAsync(json -> {
                        CompletableFuture<Void> future = new CompletableFuture<>();
                        try {
                            return this.mutationSender.sendMutationAsync(mutation, json);
                        } catch(Exception ex) {
                            future.completeExceptionally(ex);
                        }
                        return future;
                    });
        } else {
            return this.mutationSender.sendMutationAsync(mutation, null);
        }
    }

}
