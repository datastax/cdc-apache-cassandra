# Cassandra source connector for Kafka


## Build

    ./gradlew source-kafka:shadowJar

## Test

    ./gradlew clean source-kafka:test --tests com.datastax.oss.pulsar.source.CassandraSourceTests

## Start Kafka

    docker-compose up -d

Services:

    control center:  http://localhost:9021
    schema-registry: http://localhost:8081
    connect: http://localhost:8083

## Install connectors

Install the Cassandra source connector into Kafka Connect:

    curl -XPOST -H "Content-Type: application/json" "http://localhost:8083/connectors" -d @quickstart-cassandra.yaml

Install the Elasticsearch sink connector:

    curl -XPOST -H "Content-Type: application/json" "http://localhost:8083/connectors" -d @quickstart-elasticsearch.yaml

Check connector tasks:

    curl http://localhost:8083/connectors/cassandra-source/tasks
    curl http://localhost:8083/connectors/elasticsearch-sink/tasks

Uninstall connectors:

    curl -XDELETE "http://localhost:8083/connectors/cassandra-source"
    curl -XDELETE "http://localhost:8083/connectors/elasticsearch-sink"

## Check schemas

    curl http://localhost:8081/subjects/events-ks1.table1-key/versions/latest
    curl http://localhost:8081/subjects/events-ks1.table1-value/versions/latest
    curl http://localhost:8081/subjects/data-ks1.table1-key/versions/latest
    curl http://localhost:8081/subjects/data-ks1.table1-value/versions/latest

## Connector logging

Get logging levels:

    curl -Ss http://localhost:8083/admin/loggers | jq

Set looging level:

    curl -s -XPUT -H "Content-Type:application/json" \
        http://localhost:8083/admin/loggers/com.datastax.oss.kafka.source \
        -d '{"level": "DEBUG"}' | jq '.'
