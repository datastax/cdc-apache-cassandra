/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.datastax.cassandra.cdc.producer;

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
public abstract class Record implements Event {
    public final SourceInfo source;
    public final RowData rowData;
    public final Operation op;
    public final boolean shouldMarkOffset;
    public final long ts;
}
