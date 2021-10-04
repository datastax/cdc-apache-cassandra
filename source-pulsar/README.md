# Cassandra source connector for Pulsar

This Cassandra source connector reads update events from a pulsar "dirty" topic, 
read from Cassandra the updated row, and publish the Cassandra rows into a sinkable pulsar "clean" topic.

![Cassandra-source-connector](cassandra-source-connector.png)

Because connector can only write messages to one output topic, we need a "dirty" and a "clean" topic for
every replicated Cassandra tables.

![Cassandra-source-connector](cassandra-source-connector.png)


A Pulsar key-shared subscription ensure all mutations for a given primary key are processeed sequentially.

![subscription](../docs/images/subscription-key-shared.png)

## Pulsar requirements

* PR #10052 vroyer json_generic_record_builder
* Branch https://github.com/vroyer/pulsar/tree/fix/es-sink2

## Build

    ./gradlew source-pulsar:nar

## Test

    ./gradlew clean  source-pulsar:test --tests com.datastax.oss.pulsar.source.PulsarCassandraSourceTests
    
## Run

Cassandra source connector:

    $PULSAR_HOME/bin/pulsar-admin source localrun \
           --archive $CDC_HOME/source-pulsar/build/libs/pulsar-cassandra-source-<version>.nar \
           --tenant public \
           --namespace default \
           --name cassandra-source-1 \
           --destination-topic-name data-ks1.table1 \
           --source-config '{"contactPoints":"localhost", "loadBalancing.localDc":"datacenter1", "keyspace":"ks1", "table":"table1", "events.topic": "persistent://public/default/events-ks1.table1", "events.subscription.name":"sub1", "key.converter":"com.datastax.oss.pulsar.source.converters.AvroConverter","value.converter":"com.datastax.oss.pulsar.source.converters.AvroConverter"}'

Elasticsearch sink connector:

    bin/pulsar-admin sinks localrun \
    --archive pulsar-io/elastic-search/target/pulsar-io-elastic-search-2.8.0-SNAPSHOT.nar \
    --tenant public \
    --namespace default \
    --name elasticsearch-ks1-sink \
    --sink-config '{"elasticSearchUrl":"http://localhost:9200","indexName": "ks1"}' \
    --inputs data-ks1.table1 \
    --subs-name sub1
