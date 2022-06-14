# Kubernetes Quick Start

Update the gradle property **dockerRepo** in the gradle.properties file, then push a DSE image into your registry:

    ./gradlew agent-dse4:dockerPush

Install the Datastax [cass-operator](https://github.com/datastax/cass-operator):

    helm install -f cass-operator-values.yaml cass-operator datastax/cass-operator

Setup env variables:

```bash
export PROJECT_VERSION=$(cd .. && ./gradlew -q printVersion)
export CASSANDRA_CLUSTER="cluster2"
export CASSANDRA_DC="dc1"
export CASSANDRA_NAMESPACE="default"
export CASSANDRA_USERNAME=$(kubectl get secret -n $CASSANDRA_NAMESPACE ${CASSANDRA_CLUSTER}-superuser -o json | ksd | jq -r '.stringData.username')
export CASSANDRA_PASSWORD=$(kubectl get secret -n $CASSANDRA_NAMESPACE ${CASSANDRA_CLUSTER}-superuser -o json | ksd | jq -r '.stringData.password')
export CASSANDRA_SERVICE="$CASSANDRA_CLUSTER-$CASSANDRA_DC-service.$CASSANDRA_NAMESPACE.svc.cluster.local"
export PULSAR_NAMESPACE="pulsar"
export PULSAR_TOKEN=$(kubectl get secret -n $PULSAR_NAMESPACE token-admin -o json | ksd | jq -r '.stringData."admin.jwt"')
export PULSAR_ADMIN_POD=$(kubectl get pod -n $PULSAR_NAMESPACE -l component=bastion -o jsonpath='{.items[*].metadata.name}'})
export ELASTICSEARCH_URL="http://elasticsearch-master.elk.svc.cluster.local:9200"
```

Deploy a cassandra datacenter with the pulsar token:

    sed "s/{{PULSAR_TOKEN}}/$PULSAR_TOKEN/g" | \
    sed "s/{{PROJECT_VERSION}}/$PROJECT_VERSION/g" cassdc-dse-cdc.yaml | kubectl apply -f -

Wait the DC to be ready:

    while [[ $(kubectl get cassandradatacenters.cassandra.datastax.com dc1 -o json | jq -r ".status.cassandraOperatorProgress") != "Ready" ]]; do
        echo "waiting for cassandradatacenter dc1" && sleep 5;
    done

Load utilities:

    . k8s-test-lib.sh

Eventually create Pulsar partitioned topics:

    create_partitioned_topics

Create a table (otherwise cassandra-stress create the keyspace with RF=1):

    run_cqlsh "CREATE KEYSPACE IF NOT EXISTS ks1 WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'dc1':'3'};"
    run_cqlsh "CREATE TABLE IF NOT EXISTS ks1.table1 (a text PRIMARY KEY, b text) WITH cdc=true"

If the Cassandra Source Connector is not yet available in your Pulsar cluster, upload it into a broker (or bastion) pod
from where you are going to deploy the connector.

    copy_csc_nar

Deploy Cassandra and Elasticsearch connectors:

    deploy_csc
    deploy_es_sink

Stress the DSE cluster:

    kubectl exec -it pod/cluster2-dc1-rack1-sts-0 -- /opt/dse/resources/cassandra/tools/bin/cassandra-stress user profile=/table1.yaml no-warmup ops\(insert=1\) n=1000000 -rate threads=10 -node $CASSANDRA_SERVICE -mode native cql3 user=$USERNAME password=$PASSWORD

Stress the DSE cluster with a statfulset of cassandra-stress pods:

    sed -e "s/{{CASSANDRA_SERVICE}}/$CASSANDRA_SERVICE/g" \
        -e "s/{{CASSANDRA_USERNAME}}/$CASSANDRA_USERNAME/g" \
        -e "s/{{CASSANDRA_PASSWORD}}/$CASSANDRA_PASSWORD/g" cassandra-stress-sts.yaml | kubectl apply -f -

Scale up/down the cassandra-stress statefulset:

    kubectl scale statefulsets cassandra-stress --replicas=3

Check topics stats:

    topic_event_stats
    topic_data_stats

Create a port-forwarding to the grafana pod:

    GRAFANA_POD=$(kubectl get pods -n pulsar -l app.kubernetes.io/name=grafana -o jsonpath='{.items[*].metadata.name}')
    kubectl port-forward -n pulsar $GRAFANA_POD 3000:3000

Undeploy the cassandra datacenter:

    cleanup_test
    kubectl delete cassandradatacenter dc1

