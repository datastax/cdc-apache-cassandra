package com.datastax.oss.cdc.backfill;

/**
 * Dictates the cassandra image & agent to use for the test.
 */
public enum CassandraFamily {
    C3,     // Cassandra 3.11.x + agent-c3
    C4,     // Cassandra 4.x + agent-c4
    DSE4    // Datastax Enterprise Server + agent-dse4
}
