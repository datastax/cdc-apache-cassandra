/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.datastax.cassandra.cdc.producer.exceptions;

public class CassandraConnectorDataException extends RuntimeException {
    public CassandraConnectorDataException(String msg) {
        super(msg);
    }

    public CassandraConnectorDataException(Throwable t) {
        super(t);
    }

    public CassandraConnectorDataException(String msg, Throwable t) {
        super(msg, t);
    }
}
