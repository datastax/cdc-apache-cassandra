#!/usr/bin/env bash
# Step 3: write sample data into Cassandra — an insert, an update (to prove
# coalescing), and a delete (to prove tombstone propagation).
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

log "Writing sample rows into ${KEYSPACE}.${TABLE}"
cassandra_cql "INSERT INTO ${KEYSPACE}.${TABLE} (a,b) VALUES ('foo','bar');"
cassandra_cql "INSERT INTO ${KEYSPACE}.${TABLE} (a,b) VALUES ('hello','world');"
cassandra_cql "UPDATE ${KEYSPACE}.${TABLE} SET b='bar-updated' WHERE a='foo';"
cassandra_cql "INSERT INTO ${KEYSPACE}.${TABLE} (a,b) VALUES ('tmp','to-be-deleted');"
cassandra_cql "DELETE FROM ${KEYSPACE}.${TABLE} WHERE a='tmp';"

log "Allowing commitlog sync (2s) + connector read-back — waiting 5s"
sleep 5

log "Current Cassandra state:"
cassandra_cql "SELECT * FROM ${KEYSPACE}.${TABLE};"
