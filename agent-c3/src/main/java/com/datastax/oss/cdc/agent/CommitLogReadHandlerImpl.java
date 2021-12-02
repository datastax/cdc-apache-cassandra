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
package com.datastax.oss.cdc.agent;

import com.datastax.oss.cdc.agent.exceptions.CassandraConnectorSchemaException;
import com.datastax.oss.cdc.agent.exceptions.CassandraConnectorTaskException;
import lombok.extern.slf4j.Slf4j;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
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

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

import static com.datastax.oss.cdc.agent.CommitLogReadHandlerImpl.RowType.DELETE;

/**
 * Handler that implements {@link CommitLogReadHandler} interface provided by Cassandra source code.
 *
 * This handler implementation processes each {@link org.apache.cassandra.db.Mutation} and invokes one of the registered partition handler
 * for each {@link PartitionUpdate} in the {@link org.apache.cassandra.db.Mutation} (a mutation could have multiple partitions if it is a batch update),
 * which in turn makes one or more record via the {@link AbstractMutationMaker}.
 */
@Slf4j
public class CommitLogReadHandlerImpl implements CommitLogReadHandler {

    private final AbstractMutationMaker<CFMetaData, Mutation> mutationMaker;
    private final MutationSender<CFMetaData> mutationSender;
    private final SegmentOffsetWriter segmentOffsetWriter;
    private final CommitLogReaderService.Task task;
    private int markedPosition = 0;

    CommitLogReadHandlerImpl(AgentConfig config,
                             SegmentOffsetWriter segmentOffsetWriter,
                             MutationSender<CFMetaData> mutationSender,
                             CommitLogReaderService.Task task) {
        this.segmentOffsetWriter = segmentOffsetWriter;
        this.mutationSender = mutationSender;
        this.mutationMaker = new MutationMaker();
        this.task = task;
    }

    public int getMarkedPosition() {
        return this.markedPosition;
    }

    /**
     *  A PartitionType represents the type of PartitionUpdate.
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
            if (entryLocation < segmentOffsetWriter.position(Optional.empty(), descriptor.id)) {
                log.debug("Mutation at {}:{} for table {}.{} already processed, skipping...",
                        descriptor.id, entryLocation, pu.metadata().ksName, pu.metadata().cfName);
                return;
            }

            try {
                DataOutputBuffer dataOutputBuffer = new DataOutputBuffer();
                org.apache.cassandra.db.Mutation.serializer.serialize(mutation, dataOutputBuffer, descriptor.getMessagingVersion());
                String md5Digest = DigestUtils.md5Hex(dataOutputBuffer.getData());
                process(pu, descriptor.id, entryLocation, md5Digest);
            }
            catch (Exception e) {
                throw new RuntimeException(String.format("Failed to process PartitionUpdate %s at %d:%d for table %s.%s.",
                        pu.toString(), descriptor.id, entryLocation, pu.metadata().ksName, pu.metadata().cfName), e);
            }
        }
    }

    @Override
    public void handleUnrecoverableError(CommitLogReadException exception) {
        log.error("Unrecoverable error when reading commit log", exception);
        CdcMetrics.commitLogReadErrors.inc();
    }

    @Override
    public boolean shouldSkipSegmentOnError(CommitLogReadException exception) {
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
     * update is then converted into a {@link AbstractMutation}.
     */
    private void process(PartitionUpdate pu, long segment, int position, String md5Digest) {
        PartitionType partitionType = PartitionType.getPartitionType(pu);

        if (!PartitionType.isValid(partitionType)) {
            log.warn("Encountered an unsupported partition type {}, skipping...", partitionType);
            return;
        }

        switch (partitionType) {
            case PARTITION_KEY_ROW_DELETION:
            case PARTITION_AND_CLUSTERING_KEY_ROW_DELETION: {
                handlePartitionDeletion(pu, segment, position, md5Digest);
            }
            break;

            case PARTITION_LEVEL_MODIFICATION: {
                UnfilteredRowIterator it = pu.unfilteredIterator();
                Row row = it.staticRow();
                RowType rowType = RowType.getRowType(row);
                handleRowModifications(row, rowType, pu, segment, position, md5Digest);
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

                    handleRowModifications(row, rowType, pu, segment, position, md5Digest);
                }
            }
            break;

            default:
                throw new CassandraConnectorSchemaException("Unsupported partition type " + partitionType + " should have been skipped");
        }
    }

    /**
     * Handle a valid deletion event resulted from a partition-level deletion by converting Cassandra representation
     * of this event into a {@link AbstractMutation} object and send it to pulsar. A valid deletion
     * event means a partition only has a single row, this implies there are no clustering keys.
     */
    private void handlePartitionDeletion(PartitionUpdate pu, long segment, int position, String md5Digest) {
        try {
            Object[] after = new Object[pu.metadata().partitionKeyColumns().size() + pu.metadata().clusteringColumns().size()];
            populatePartitionColumns(after, pu);
            mutationMaker.delete(StorageService.instance.getLocalHostUUID(), segment, position,
                    pu.maxTimestamp(), after, this::sendAsync, md5Digest, pu.metadata(), pu.partitionKey().getToken().getTokenValue());
        }
        catch (Exception e) {
            log.error("Fail to send delete partition at {}:{}. Reason: {}", segment, position, e);
        }
    }

    /**
     * Handle a valid event resulted from a row-level modification by converting Cassandra representation of
     * this event into a {@link AbstractMutation} object and sent it to pulsar. A valid event
     * implies this must be an insert, update, or delete.
     */
    private void handleRowModifications(Row row, RowType rowType, PartitionUpdate pu,
                                        long segment, int position, String md5Digest) {
        Object[] after = new Object[pu.metadata().partitionKeyColumns().size() + pu.metadata().clusteringColumns().size()];
        populatePartitionColumns(after, pu);
        populateClusteringColumns(after, row, pu);

        long ts = rowType == DELETE ? row.deletion().time().markedForDeleteAt() : pu.maxTimestamp();
        switch (rowType) {
            case INSERT:
                mutationMaker.insert(StorageService.instance.getLocalHostUUID(), segment, position,
                        ts, after, this::sendAsync, md5Digest, pu.metadata(), pu.partitionKey().getToken().getTokenValue());
                break;

            case UPDATE:
                mutationMaker.update(StorageService.instance.getLocalHostUUID(), segment, position,
                        ts, after, this::sendAsync, md5Digest, pu.metadata(), pu.partitionKey().getToken().getTokenValue());
                break;

            case DELETE:
                mutationMaker.delete(StorageService.instance.getLocalHostUUID(), segment, position,
                        ts, after, this::sendAsync, md5Digest, pu.metadata(), pu.partitionKey().getToken().getTokenValue());
                break;

            default:
                throw new CassandraConnectorTaskException("Unsupported row type " + rowType + " should have been skipped");
        }
    }

    private void populatePartitionColumns(Object[] after, PartitionUpdate pu) {
        List<Object> partitionKeys = getPartitionKeys(pu);
        int  i = 0;
        for (ColumnDefinition cd : pu.metadata().partitionKeyColumns()) {
            try {
                after[i++] = partitionKeys.get(cd.position());
            }
            catch (Exception e) {
                throw new RuntimeException(String.format("Failed to populate Column %s with Type %s of Table %s in KeySpace %s.",
                        cd.name.toString(), cd.type.toString(), cd.cfName, cd.ksName), e);
            }
        }
    }

    private void populateClusteringColumns(Object[] after, Row row, PartitionUpdate pu) {
        int  i = pu.metadata().partitionKeyColumns().size();
        for (ColumnDefinition cd : pu.metadata().clusteringColumns().stream().limit(row.clustering().size()).collect(Collectors.toList())) {
            try {
                after[i++] = cd.type.compose(row.clustering().get(cd.position()));
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

    public void sendAsync(Mutation mutation) {
        log.debug("Sending mutation={}", mutation);
        try {
            task.inflightMessagesSemaphore.acquireUninterruptibly(); // may block
            this.mutationSender.sendMutationAsync(mutation)
                    .handle((msgId, t)-> {
                        if (t == null) {
                            CdcMetrics.sentMutations.inc();
                            log.debug("Sent mutation={}", mutation);
                        } else {
                            if (t instanceof CassandraConnectorSchemaException) {
                                log.error("Invalid primary key schema:", t);
                                CdcMetrics.skippedMutations.inc();
                            } else {
                                CdcMetrics.sentErrors.inc();
                                log.debug("Sent failed mutation=" + mutation, t);
                                task.lastException = t;
                            }
                        }
                        task.inflightMessagesSemaphore.release();
                        return msgId;
                    });
        } catch(Exception e) {
            log.error("Send failed:", e);
            CdcMetrics.sentErrors.inc();
        }
    }
}
