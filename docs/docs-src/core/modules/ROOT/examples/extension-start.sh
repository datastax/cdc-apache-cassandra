./bin/pulsar-admin cassandra-cdc backfill --data-dir target/export --export-host 127.0.0.1:9042 \
 --export-username cassandra --export-password cassandra --keyspace ks1 --table table1