/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.datastax.cassandra.cdc.producer;

import com.datastax.cassandra.cdc.MutationKey;
import com.datastax.cassandra.cdc.MutationValue;
import com.datastax.cassandra.cdc.Operation;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;


/**
 * An immutable data structure representing a change event, and can be converted
 * to a kafka connect Struct representing key/value of the change event.
 */
@Getter
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class Mutation {
    private long segment;
    private int position;
    private SourceInfo source;
    private RowData rowData;
    private Operation op;
    private boolean shouldMarkOffset;
    private long ts;

    public MutationKey mutationKey() {
        return new MutationKey(
                source.keyspaceTable.keyspace,
                source.keyspaceTable.table,
                rowData.primaryKeyValues());
    }

    public MutationValue mutationValue(String jsonDocument) {
        return new MutationValue(source.timestamp.toEpochMilli(), source.nodeId, op, jsonDocument);
    }
}
