# Cassandra source connector

This Cassandra source connector reads update events from a pulsar "dirty" topic, 
read from Cassandra the updated row, and publish the Cassandra rows into a sinkable pulsar "clean" topic.

![Cassandra-source-connector](cassandra-source-connector.png)

## Build

    ./gradlew pulsar-source:nar
    
## Run

    /Users/vroyer/git/apache/pulsar/bin/pulsar-admin source localrun \
           --archive /Users/vroyer/git/datastax/cassandra-source-connector/pulsar-source/build/libs/pulsar-source-0.1-SNAPSHOT.nar \
           --tenant public \
           --namespace default \
           --name cassandra-source-1 \
           --destination-topic-name elasticsearch-ks1-sink \
           --source-config '{"contactPoints":"localhost:9042", "localDc":"datacenter1", "dirtyTopicName": "persistent://public/default/topic1", "dirtySubscriptionName":"sub1", "converter":"com.datastax.oss.pulsar.source.JsonStringConverter"}'
