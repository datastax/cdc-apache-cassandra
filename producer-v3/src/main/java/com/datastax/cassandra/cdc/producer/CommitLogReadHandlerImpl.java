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
package com.datastax.cassandra.cdc.producer;

import com.datastax.cassandra.cdc.producer.exceptions.CassandraConnectorSchemaException;
import com.datastax.cassandra.cdc.producer.exceptions.CassandraConnectorTaskException;
import lombok.extern.slf4j.Slf4j;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.commitlog.CommitLogDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogReadHandler;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import static com.datastax.cassandra.cdc.producer.CommitLogReadHandlerImpl.RowType.DELETE;

/**
 * Handler that implements {@link CommitLogReadHandler} interface provided by Cassandra source code.
 *
 * This handler implementation processes each {@link org.apache.cassandra.db.Mutation} and invokes one of the registered partition handler
 * for each {@link PartitionUpdate} in the {@link org.apache.cassandra.db.Mutation} (a mutation could have multiple partitions if it is a batch update),
 * which in turn makes one or more record via the {@link MutationMaker}.
 */
@Slf4j
public class CommitLogReadHandlerImpl implements CommitLogReadHandler {
    private static final boolean MARK_OFFSET = true;

    private final MutationMaker<CFMetaData> mutationMaker;
    private final MutationSender<CFMetaData> mutationSender;
    private final OffsetWriter offsetWriter;

    CommitLogReadHandlerImpl(ProducerConfig config,
                             OffsetFileWriter offsetFileWriter,
                             MutationSender<CFMetaData> mutationSender) {
        this.mutationSender = mutationSender;
        this.mutationMaker = new MutationMaker<>(config);
        this.offsetWriter = offsetFileWriter;
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
        COUNTER,

        /**
         * a partition-level modification
         */
        PARTITION_LEVEL_MODIFICATION;

        static final Set<PartitionType> supportedPartitionTypes = new HashSet<>(Arrays.asList(
                PARTITION_KEY_ROW_DELETION,
                PARTITION_AND_CLUSTERING_KEY_ROW_DELETION,
                ROW_LEVEL_MODIFICATION,
                PARTITION_LEVEL_MODIFICATION));

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
            else if (!pu.unfilteredIterator().hasNext()) {
                return PARTITION_LEVEL_MODIFICATION;
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
                return getRowType(row);
            }
            return UNKNOWN;
        }

        public static RowType getRowType(Row row) {
            if (isDelete(row)) {
                return DELETE;
            }
            else if (isInsert(row)) {
                return INSERT;
            }
            else if (isUpdate(row)) {
                return UPDATE;
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
        if (!mutation.trackedByCDC()) {
            return;
        }

        for (PartitionUpdate pu : mutation.getPartitionUpdates()) {
            com.datastax.cassandra.cdc.producer.CommitLogPosition entryPosition =
                    new com.datastax.cassandra.cdc.producer.CommitLogPosition(CommitLogUtil.extractTimestamp(descriptor.fileName()), entryLocation);

            if (offsetWriter.offset().compareTo(entryPosition) > 0) {
                log.debug("Mutation at {} for table {}.{} already processed, skipping...",
                        entryPosition, pu.metadata().ksName, pu.metadata().cfName);
                return;
            }

            try {
                DataOutputBuffer dataOutputBuffer = new DataOutputBuffer();
                org.apache.cassandra.db.Mutation.serializer.serialize(mutation, dataOutputBuffer, descriptor.getMessagingVersion());
                String md5Digest = DigestUtils.md5Hex(dataOutputBuffer.getData());
                process(pu, entryPosition, md5Digest);
            }
            catch (Exception e) {
                throw new RuntimeException(String.format("Failed to process PartitionUpdate %s at %s for table %s.%s.",
                        pu.toString(), entryPosition, pu.metadata().ksName, pu.metadata().cfName), e);
            }
        }
    }

    @Override
    public void handleUnrecoverableError(CommitLogReadException exception) throws IOException {
        log.error("Unrecoverable error when reading commit log", exception);
        CdcMetrics.commitLogReadErrors.inc();
    }

    @Override
    public boolean shouldSkipSegmentOnError(CommitLogReadException exception) throws IOException {
        if (exception.permissible) {
            log.error("Encountered a permissible exception during log replay", exception);
        }
        else {
            log.error("Encountered a non-permissible exception during log replay", exception);
        }
        return false;
    }

    /**
     * Method which processes a partition update if it's valid (either a single-row partition-level
     * deletion or a row-level modification) or throw an exception if it isn't. The valid partition
     * update is then converted into a {@link Mutation}.
     */
    private void process(PartitionUpdate pu, com.datastax.cassandra.cdc.producer.CommitLogPosition position, String md5Digest) {
        PartitionType partitionType = PartitionType.getPartitionType(pu);

        if (!PartitionType.isValid(partitionType)) {
            log.warn("Encountered an unsupported partition type {}, skipping...", partitionType);
            return;
        }

        switch (partitionType) {
            case PARTITION_KEY_ROW_DELETION:
            case PARTITION_AND_CLUSTERING_KEY_ROW_DELETION: {
                handlePartitionDeletion(pu, position, md5Digest);
            }
            break;

            case PARTITION_LEVEL_MODIFICATION: {
                UnfilteredRowIterator it = pu.unfilteredIterator();
                Row row = it.staticRow();
                RowType rowType = RowType.getRowType(row);
                handleRowModifications(row, rowType, pu, position, md5Digest);
            }
            break;

            case ROW_LEVEL_MODIFICATION: {
                UnfilteredRowIterator it = pu.unfilteredIterator();
                while (it.hasNext()) {
                    Unfiltered rowOrRangeTombstone = it.next();
                    RowType rowType = RowType.getRowType(rowOrRangeTombstone);
                    if (!RowType.isValid(rowType)) {
                        log.warn("Encountered an unsupported row type {}, skipping...", rowType);
                        continue;
                    }
                    Row row = (Row) rowOrRangeTombstone;

                    handleRowModifications(row, rowType, pu, position, md5Digest);
                }
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
    private void handlePartitionDeletion(PartitionUpdate pu, com.datastax.cassandra.cdc.producer.CommitLogPosition offsetPosition, String md5Digest) {
        try {
            RowData after = new RowData();
            populatePartitionColumns(after, pu);
            mutationMaker.delete(DatabaseDescriptor.getClusterName(), StorageService.instance.getLocalHostUUID(), offsetPosition,
                    pu.metadata().ksName, pu.metadata().cfName, false,
                    pu.maxTimestamp(), after,
                    MARK_OFFSET, this::blockingSend, md5Digest, pu.metadata());
        }
        catch (Exception e) {
            log.error("Fail to send delete partition at {}. Reason: {}", offsetPosition, e);
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
    private void handleRowModifications(Row row, RowType rowType, PartitionUpdate pu,
                                        com.datastax.cassandra.cdc.producer.CommitLogPosition offsetPosition, String md5Digest) {
        RowData after = new RowData();
        populatePartitionColumns(after, pu);
        populateClusteringColumns(after, row, pu);

        long ts = rowType == DELETE ? row.deletion().time().markedForDeleteAt() : pu.maxTimestamp();
        switch (rowType) {
            case INSERT:
                mutationMaker.insert(DatabaseDescriptor.getClusterName(), StorageService.instance.getLocalHostUUID(), offsetPosition,
                        pu.metadata().ksName, pu.metadata().cfName, false,
                        ts, after, MARK_OFFSET, this::blockingSend, md5Digest, pu.metadata());
                break;

            case UPDATE:
                mutationMaker.update(DatabaseDescriptor.getClusterName(), StorageService.instance.getLocalHostUUID(), offsetPosition,
                        pu.metadata().ksName, pu.metadata().cfName, false,
                        ts, after, MARK_OFFSET, this::blockingSend, md5Digest, pu.metadata());
                break;

            case DELETE:
                mutationMaker.delete(DatabaseDescriptor.getClusterName(), StorageService.instance.getLocalHostUUID(), offsetPosition,
                        pu.metadata().ksName, pu.metadata().cfName, false,
                        ts, after, MARK_OFFSET, this::blockingSend, md5Digest, pu.metadata());
                break;

            default:
                throw new CassandraConnectorTaskException("Unsupported row type " + rowType + " should have been skipped");
        }
    }

    private void populatePartitionColumns(RowData after, PartitionUpdate pu) {
        List<Object> partitionKeys = getPartitionKeys(pu);
        for (ColumnDefinition cd : pu.metadata().partitionKeyColumns()) {
            try {
                String name = cd.name.toString();
                Object value = partitionKeys.get(cd.position());
                CellData cellData = new CellData(name, value, null, CellData.ColumnType.PARTITION);
                after.addCell(cellData);
            }
            catch (Exception e) {
                throw new RuntimeException(String.format("Failed to populate Column %s with Type %s of Table %s in KeySpace %s.",
                        cd.name.toString(), cd.type.toString(), cd.cfName, cd.ksName), e);
            }
        }
    }

    @SuppressWarnings({"unchecked","rawtypes"})
    private void populateClusteringColumns(RowData after, Row row, PartitionUpdate pu) {
        for (ColumnDefinition cd : pu.metadata().clusteringColumns().stream().limit(row.clustering().size()).collect(Collectors.toList())) {
            try {
                String name = cd.name.toString();
                Object value = cd.type.compose(row.clustering().get(cd.position()));
                CellData cellData = new CellData(name, value, null, CellData.ColumnType.CLUSTERING);
                after.addCell(cellData);
            }
            catch (Exception e) {
                throw new RuntimeException(String.format("Failed to populate Column %s with Type %s of Table %s in KeySpace %s.",
                        cd.name.toString(), cd.type.toString(), cd.cfName, cd.ksName), e);
            }
        }
    }

    /**
     * Given a PartitionUpdate, deserialize the partition key byte buffer
     * into a list of partition key values.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    private static List<Object> getPartitionKeys(PartitionUpdate pu) {
        List<Object> values = new ArrayList<>();

        List<ColumnDefinition> columnDefinitions = pu.metadata().partitionKeyColumns();

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
                throw new RuntimeException(String.format("Failed to deserialize Column %s with Type %s in Table %s and KeySpace %s.",
                        cs.name.toString(), cs.type.toString(), cs.cfName, cs.ksName), e);
            }
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
                    throw new RuntimeException(String.format("Failed to deserialize Column %s with Type %s in Table %s and KeySpace %s",
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

    public void blockingSend(Mutation<CFMetaData> mutation) {
        com.datastax.cassandra.cdc.producer.CommitLogPosition sentOffset = offsetWriter.offset();
        long seg = sentOffset.segmentId;
        int pos = sentOffset.position;

        assert mutation != null : "Unexpected null mutation";
        assert mutation.getCommitLogPosition().getSegmentId() >= seg : "Unexpected mutation segment";
        assert mutation.getCommitLogPosition().getSegmentId() > seg ||
                (mutation.getCommitLogPosition().getSegmentId() == seg && mutation.getCommitLogPosition().getPosition() > pos)
                : "Unexpected mutation offset";

        log.debug("Sending mutation={}", mutation);

        while(true) {
            try {
                processMutation(mutation).toCompletableFuture().get();
                break;
            } catch(Exception e) {
                log.error("failed to send message to pulsar:", e);
                CdcMetrics.sentErrors.inc();
                try {
                    Thread.sleep(10000);
                } catch(InterruptedException interruptedException) {
                }
            }
        }
    }

    // TODO: add exponential retry
    CompletionStage<Void> processMutation(final Mutation<CFMetaData> mutation) throws Exception {
        return this.mutationSender.sendMutationAsync(mutation)
                .thenAccept(msgId -> {
                    if (msgId != null) {
                        CdcMetrics.sentMutations.inc();
                        offsetWriter.markOffset(mutation);
                        log.info("mutation={} sent", mutation);
                    }
                });
    }
}
