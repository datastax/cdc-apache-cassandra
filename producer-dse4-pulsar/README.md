# DSE CDC producer for Pulsar

## Build

    ./gradlew producer-dse4-pulsar:shadowJar

## Run

    export JVM_EXTRA_OPTS="-javaagent:$CSC_HOME/producer-dse4-pulsar/build/libs/producer-dse4-pulsar-<version>-SNAPSHOT-all.jar=pulsarServiceUrl=pulsar://pulsar:6650,cdcWorkingDir=/var/lib/cassandra/cdc"

## Docker Testing

Build DSE docker image with CDC enabled:

    docker build -t dse-server:6.8.16-cdc -t vroyer/dse-server:latest -f producer-dse4-pulsar/Dockerfile.dse .

Build LunaStreaming image with the Cassandra source connector:

    docker build -t lunastreaming:2.7.2_1.1.6-csc -f producer-dse4-pulsar/Dockerfile.luna .

Run DSE and LunaStreaming:

    docker-compose -f producer-dse4-pulsar/docker-compose.yaml up -d

Stress the DSE node:

    docker exec -it dse /opt/dse/resources/cassandra/tools/bin/cassandra-stress user profile=/table1.yaml no-warmup ops\(insert=1\) n=1000000 -rate threads=10

Deploy the Cassandra Source Connector:

    docker exec -it luna bin/pulsar-admin source create \
    --archive /pulsar-cassandra-source-0.2.7-SNAPSHOT.nar \
    --tenant public \
    --namespace default \
    --name cassandra-source-ks1-table1 \
    --destination-topic-name data-ks1.table1 \
    --source-config "{
      \"keyspace\": \"ks1\",
      \"table\": \"table1\",
      \"events.topic\": \"persistent://public/default/events-ks1.table1\",
      \"events.subscription.name\": \"sub1\",
      \"key.converter\": \"com.datastax.oss.pulsar.source.converters.AvroConverter\",
      \"value.converter\": \"com.datastax.oss.pulsar.source.converters.AvroConverter\",
      \"contactPoints\": \"dse\",
      \"loadBalancing.localDc\": \"dc1\",
      \"auth.provider\": \"PLAIN\",
      \"auth.username\": \"cassandra\",
      \"auth.password\": \"cassandra\"
    }"

Check connector status:

    docker exec -it luna bin/pulsar-admin source status --name cassandra-source-ks1-table1

docker exec -it luna bin/pulsar-admin source delete --tenant public --namespace default --name cassandra-source-ks1-table1

Check source connector logs:

    docker exec -it luna cat /pulsar/logs/functions/public/default/cassandra-source-ks1-table1/cassandra-source-ks1-table1-0.log

Check events topics throughput:

    docker exec -it luna bin/pulsar-admin topics stats persistent://public/default/events-ks1.table1
    {
        "msgRateIn" : 197.18335732397514,
        "msgThroughputIn" : 54891.20667843014,
        "msgRateOut" : 0.0,
        "msgThroughputOut" : 0.0,
        "bytesInCounter" : 355997236,
        "msgInCounter" : 1276328,
        "bytesOutCounter" : 0,
        "msgOutCounter" : 0,
        "averageMsgSize" : 278.37646859944215,
        "msgChunkPublished" : false,
        "storageSize" : 21242684,
        "backlogSize" : 0,
        "offloadedStorageSize" : 0,
        "publishers" : [ {
        "msgRateIn" : 197.18335732397514,
        "msgThroughputIn" : 54891.20667843014,
        "averageMsgSize" : 278.0,
        "chunkedMessageRate" : 0.0,
        "producerId" : 0,
        "metadata" : { },
        "producerName" : "pulsar-producer-f2c3cc84-03af-49b3-8f71-4691ed449005-events-ks1.table1",
        "connectedSince" : "2021-10-19T09:33:13.752189Z",
        "clientVersion" : "2.7.2.1.1.6",
        "address" : "/192.168.192.3:44436"
        } ],
        "subscriptions" : { },
        "replication" : { },
        "deduplicationStatus" : "Disabled",
        "nonContiguousDeletedMessagesRanges" : 0,
        "nonContiguousDeletedMessagesRangesSerializedSize" : 0
    }

Check data topics throughput:

    docker exec -it luna bin/pulsar-admin topics stats persistent://public/default/data-ks1.table1
    {
        "msgRateIn" : 453.3253102308844,
        "msgThroughputIn" : 30574.775544097494,
        "msgRateOut" : 0.0,
        "msgThroughputOut" : 0.0,
        "bytesInCounter" : 3317974,
        "msgInCounter" : 48800,
        "bytesOutCounter" : 0,
        "msgOutCounter" : 0,
        "averageMsgSize" : 67.44555147058824,
        "msgChunkPublished" : false,
        "storageSize" : 3317974,
        "backlogSize" : 0,
        "offloadedStorageSize" : 0,
        "publishers" : [ {
            "msgRateIn" : 453.3253102308844,
            "msgThroughputIn" : 30574.775544097494,
            "averageMsgSize" : 67.0,
            "chunkedMessageRate" : 0.0,
            "producerId" : 0,
            "metadata" : {
            "instance_id" : "0",
            "application" : "pulsar-source",
            "instance_hostname" : "101f194b9c40",
            "id" : "public/default/cassandra-source-ks1-table1"
        },
        "producerName" : "standalone-0-0",
        "connectedSince" : "2021-10-19T13:18:43.858727Z",
        "clientVersion" : "2.7.2.1.1.6",
        "address" : "/127.0.0.1:47356"
        } ],
        "subscriptions" : { },
        "replication" : { },
        "deduplicationStatus" : "Disabled",
        "nonContiguousDeletedMessagesRanges" : 0,
        "nonContiguousDeletedMessagesRangesSerializedSize" : 0
    }

Shutdown nodes:

    docker-compose stop; docker rm -f dse luna

## Kubernetes testing

Push DSE image into your registry:

    docker push xxxxx/dse-server:latest

Install the datastax cassoperator:

    helm install -f cass-operator-values.yaml cass-operator datastax/cass-operator

Deploy a cassandra datacenter with the pulsar token:

    PULSAR_TOKEN=$(kubectl get secret -n pulsar token-admin -o json | ksd | jq -r '.stringData."admin.jwt"')
    sed "s/{{PULSAR_TOKEN}}/$PULSAR_TOKEN/g" cassdc-dse-cdc.yaml | kubectl apply -f -
    kubectl apply -f service_monitor.yaml

Wait the DC to be ready:

    while [[ $(kubectl get cassandradatacenters.cassandra.datastax.com dc1 -o json | jq -r ".status.cassandraOperatorProgress") != "Ready" ]]; do
        echo "waiting for cassandradatacenter dc1" && sleep 5;
    done

Load utilities:

    . k8s-test-lib.sh

Eventually create partitioned topics:

    create_partitioned_topics

Create a table:

    run_cqlsh "CREATE KEYSPACE IF NOT EXISTS ks1 WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'dc1':'3'};"
    run_cqlsh "CREATE TABLE IF NOT EXISTS ks1.table1 (a text PRIMARY KEY, b text) WITH cdc=true"

Deploy Cassandra and Elasticsearch connectors:

    deploy_csc
    deploy_es_sink

Stress the DSE cluster:

    kubectl exec -it pod/cluster2-dc1-rack1-sts-0 -- /opt/dse/resources/cassandra/tools/bin/cassandra-stress user profile=/table1.yaml no-warmup ops\(insert=1\) n=1000000 -rate threads=10 -node $CASSANDRA_SERVICE -mode native cql3 user=$USERNAME password=$PASSWORD

Check topics stats:

    topic_event_stats
    topic_data_stats

Undeploy the cassandra datacenter:

    cleanup_test
    kubectl delete cassandradatacenter dc1

