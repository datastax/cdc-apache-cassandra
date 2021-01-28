/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.datastax.cassandra.cdc.producer;

import com.google.common.collect.ImmutableList;
import io.micrometer.core.instrument.Tag;

import java.util.List;
import java.util.Objects;

/**
 * The KeyspaceTable uniquely identifies each table in the Cassandra cluster
 */
public class KeyspaceTable {
    public final String keyspace;
    public final String table;

    public KeyspaceTable(String keyspace, String table) {
        this.keyspace = keyspace;
        this.table = table;
    }

    public String name() {
        return keyspace + "." + table;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KeyspaceTable that = (KeyspaceTable) o;
        return keyspace.equals(that.keyspace) && table.equals(that.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyspace, table);
    }

    @Override
    public String toString() {
        return name();
    }

    public String identifier() {
        return keyspace + "." + table;
    }

    public List<Tag> tags() {
        return ImmutableList.of(Tag.of("keyspace", keyspace), Tag.of("table", table));
    }
}
