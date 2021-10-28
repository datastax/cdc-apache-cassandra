# Kubernetes testing

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

