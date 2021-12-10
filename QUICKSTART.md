# Docker Quick Start

## Build docker images

Build docker images with CDC enabled:

    ./gradlew clean build -x test

## Start Cassandra and Pulsar

Start containers for Cassandra 3.11 (c3), Cassandra 4.0 (c4), or DSE 6.8.16+ (dse4) at your convenience, and Apache Pulsar:

    ./gradlew agent-dse4-luna:composeUp
    ./gradlew agent-c4-luna:composeUp
    ./gradlew agent-c3-luna:composeUp

Create the keyspace and table:

    docker exec -it cassandra cqlsh -e "CREATE KEYSPACE ks1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};"
    docker exec -it cassandra cqlsh -e "CREATE TABLE ks1.table1 (a text, b text, PRIMARY KEY (a)) WITH cdc=true;"

Deploy a Cassandra Source Connector in the pulsar container:

    docker exec -it pulsar bin/pulsar-admin source create \
    --source-type cassandra-source \
    --tenant public \
    --namespace default \
    --name cassandra-source-ks1-table1 \
    --destination-topic-name data-ks1.table1 \
    --source-config "{
      \"keyspace\": \"ks1\",
      \"table\": \"table1\",
      \"events.topic\": \"persistent://public/default/events-ks1.table1\",
      \"events.subscription.name\": \"sub1\",
      \"contactPoints\": \"cassandra\",
      \"loadBalancing.localDc\": \"datacenter1\"
    }"

Check the source connector status (should be running):

    docker exec -it pulsar bin/pulsar-admin source status --name cassandra-source-ks1-table1

Check the source connector logs:

    docker exec -it pulsar cat /pulsar/logs/functions/public/default/cassandra-source-ks1-table1/cassandra-source-ks1-table1-0.log

Stress the Cassandra node:

    docker exec -it cassandra cassandra-stress user profile=/table1.yaml no-warmup ops\(insert=1\) n=1000000 -rate threads=10

For Cassandra 3 only, we need to fill up the working commitlog file (the one where new mutations are appended) and flush the table to get discarded commitlog files visible from the /var/lib/cassandra/cdc_raw directory:

    docker exec -it cassandra cassandra-stress user profile=/table2.yaml no-warmup ops\(insert=1\) n=1000000 -rate threads=10
    docker exec -it cassandra nodetool flush

Check events and data topics throughput:

    docker exec -it pulsar bin/pulsar-admin topics stats persistent://public/default/events-ks1.table1
    docker exec -it pulsar bin/pulsar-admin topics stats persistent://public/default/data-ks1.table1

Check the cassandra source connector metrics:

    docker exec -it pulsar curl http://localhost:8080/metrics/ | grep cassandra-source-ks1-table1

## Prometheus monitoring

Start the prometheus and grafana containers to monitor the CDC replication:

    ./gradlew agent-dse4-pulsar:prometheusComposeUp
    
Open [prometheus](http://localhost:9090) and [grafana](http://localhost:3000) (login=admin, password=admin)

## Elasticsearch sink

Start elasticsearch and kibana containers:

    ./gradlew agent-dse4-luna:elasticsearchComposeUp

Deploy an Elasticsearch sink connector:

    docker exec -it pulsar bin/pulsar-admin sink create \
    --sink-type elastic_search \
    --tenant public \
    --namespace default \
    --name es-sink-ks1-table1 \
    --inputs "persistent://public/default/data-ks1.table1" \
    --subs-position Earliest \
    --sink-config "{
      \"elasticSearchUrl\":\"http://elasticsearch:9200\",
      \"indexName\":\"ks1.table1\",
      \"keyIgnore\":\"false\",
      \"nullValueAction\":\"DELETE\",
      \"schemaEnable\":\"true\"
    }"

Check the sink connector status (should be running):

    docker exec -it pulsar bin/pulsar-admin sink status --name es-sink-ks1-table1

Check the source connector logs:

    docker exec -it pulsar cat /pulsar/logs/functions/public/default/es-sink-ks1-table1/es-sink-ks1-table1.log

Check data are replicated in [elasticsearch](http://localhost:9200/_cat/indices)

    curl http://localhost:9200/_cat/indices

## Shutdown containers

    ./gradlew agent-dse4-luna:composeDown
    ./gradlew agent-c4-luna:composeDown
    ./gradlew agent-c3-luna:composeDown
