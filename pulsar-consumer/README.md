# Pulsar consumer

Consume Cassandra row messages from a pulsar topic in order to read the last value, and publish to a Pulsar sink topic.

## Run

    ./gradlew pulsar-consumer:run
    
## Elasticsearch sink example

    bin/pulsar-admin sinks localrun \
        --archive pulsar-io/elastic-search/target/pulsar-io-elastic-search-2.8.0-SNAPSHOT.nar \
        --tenant public \
        --namespace default \
        --name elasticsearch-ks1-sink \
        --sink-config '{"elasticSearchUrl":"http://localhost:9200","indexName": "ks1"}' \
        --inputs elasticsearch-ks1-sink

