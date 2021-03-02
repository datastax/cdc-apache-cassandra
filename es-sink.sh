/Users/vroyer/apache-pulsar-2.7.0/bin/pulsar-admin sinks localrun \
    --archive connectors/pulsar-io-elastic-search-2.7.0.nar \
    --tenant public \
    --namespace default \
    --name elasticsearch-ks1-sink \
    --sink-config '{"elasticSearchUrl":"http://localhost:9200","indexName": "ks1"}' \
    --inputs elasticsearch-ks1-sink