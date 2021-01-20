/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.datastax.cassandra.cdc.producer;

/**
 * Cell-level data about the source event. Each cell contains the name, value and
 * type of a column in a Cassandra table.
 */
public class CellData {

    /**
     * The type of a column in a Cassandra table
     */
    public enum ColumnType {
        /**
         * A partition column is responsible for data distribution across nodes for this table.
         * Every Cassandra table must have at least one partition column.
         */
        PARTITION,

        /**
         * A clustering column is used to specifies the order that the data is arranged inside the partition.
         * A Cassandra table may not have any clustering column,
         */
        CLUSTERING,

        /**
         * A regular column is a column that is not a partition or a clustering column.
         */
        REGULAR
    }

    public final String name;
    public final Object value;
    public final Object deletionTs;
    public final ColumnType columnType;

    public CellData(String name, Object value, Object deletionTs, ColumnType columnType) {
        this.name = name;
        this.value = value;
        this.deletionTs = deletionTs;
        this.columnType = columnType;
    }

    public boolean isPrimary() {
        return columnType == ColumnType.PARTITION || columnType == ColumnType.CLUSTERING;
    }

    public String toString() {
        return name + "=" + value;
    }
}
