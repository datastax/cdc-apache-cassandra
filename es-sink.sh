/Users/vroyer/git/apache/pulsar/bin/pulsar-admin sinks localrun \
    --archive pulsar-io/elastic-search/target/pulsar-io-elastic-search-2.8.0-SNAPSHOT.nar \
    --tenant public \
    --namespace default \
    --name elasticsearch-ks1-sink \
    --sink-config '{"elasticSearchUrl":"http://localhost:9200","indexName": "ks1"}' \
    --inputs data-ks1.table1 \
    --subs-name sub1
