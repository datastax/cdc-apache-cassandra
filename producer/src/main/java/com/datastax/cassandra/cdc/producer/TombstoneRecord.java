/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.datastax.cassandra.cdc.producer;

import com.datastax.cassandra.cdc.Operation;

/**
 * A TombstoneRecord is a record which has the same key as a delete event but has null value.
 * With null value, Kafka knows that it can remove messages with the same key for log compaction.
 */
public class TombstoneRecord extends Record {

    public TombstoneRecord(SourceInfo source, RowData rowData) {
        super(source, rowData, Operation.DELETE, false, System.currentTimeMillis());
    }

    @Override
    public EventType getEventType() {
        return EventType.TOMBSTONE_EVENT;
    }
}
