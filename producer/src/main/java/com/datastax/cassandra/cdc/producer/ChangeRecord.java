/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.datastax.cassandra.cdc.producer;

import com.datastax.cassandra.cdc.Operation;

/**
 * An internal representation of a create/update/delete event.
 */
public class ChangeRecord extends Record {

    public ChangeRecord(SourceInfo source, RowData rowData, Operation op, boolean markOffset) {
        super(source, rowData, op, markOffset, System.currentTimeMillis());
    }

    @Override
    public EventType getEventType() {
        return EventType.CHANGE_EVENT;
    }
}
