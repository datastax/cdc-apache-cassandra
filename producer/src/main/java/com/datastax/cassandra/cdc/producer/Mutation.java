/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.datastax.cassandra.cdc.producer;

import com.datastax.cassandra.cdc.MutationValue;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.List;


/**
 * An immutable data structure representing a change event, and can be converted
 * to a kafka connect Struct representing key/value of the change event.
 */
@Getter
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class Mutation<T> {
    private CommitLogPosition commitLogPosition;
    private SourceInfo source;
    private RowData rowData;
    private boolean shouldMarkOffset;
    private long ts;
    private String md5Digest;
    private T metadata;

    public List<CellData> primaryKeyCells() {
        return rowData.primaryKeyCells();
    }

    public MutationValue mutationValue() {
        // TODO: Unfortunately, computing the mutation CRC require to re-serialize it because we cannot get the byte[] from the commitlog reader.
        // So, we use the timestamp here.
        return new MutationValue(md5Digest, source.nodeId, rowData.nonPrimaryKeyNames());
    }
}
