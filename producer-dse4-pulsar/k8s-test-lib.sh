#!/usr/bin/env bash
#
# Basic test of the full CDC pipeline from DSE to Elasticsearch through Pulsar,
# running in a k8s pod when DSE, Pulsar and ES are installed.
#
# Requires curl, cqlsh, pulsar-admin, jq, jd (https://github.com/josephburnett/jd), see the Dockerfile.
set -x

USERNAME=$(kubectl get secret -n default cluster2-superuser -o json | ksd | jq -r '.stringData.username')
PASSWORD=$(kubectl get secret -n default cluster2-superuser -o json | ksd | jq -r '.stringData.password')
PULSAR_TOKEN=$(kubectl get secret -n pulsar token-admin -o json | ksd | jq -r '.stringData."admin.jwt"')

PULSAR_BROKER="ssl+pulsar://pulsar-perf-aws-useast2-broker.pulsar.svc.cluster.local:6650"
PULSAR_BROKER_HTTP="https://pulsar-perf-aws-useast2-proxy.pulsar.svc.cluster.local:8443/"
PULSAR_ADMIN_AUTH="--auth-plugin org.apache.pulsar.client.impl.auth.AuthenticationToken --auth-params token:$PULSAR_TOKEN"

CASSANDRA_SERVICE="cluster2-dc1-service.default.svc.cluster.local"
CASSANDRA_DC="dc1"

ELASTICSEARCH_URL="http://elasticsearch-master.elk.svc.cluster.local:9200"

pulsar_configure() {
  kubectl exec -it pod/cluster2-dc1-rack1-sts-0 -- /apache-pulsar-2.8.0/bin/pulsar-admin --admin-url $PULSAR_BROKER_HTTP $PULSAR_ADMIN_AUTH namespaces set-auto-topic-creation public/default --enable
  kubectl exec -it pod/cluster2-dc1-rack1-sts-0 -- /apache-pulsar-2.8.0/bin/pulsar-admin --admin-url $PULSAR_BROKER_HTTP $PULSAR_ADMIN_AUTH namespaces set-is-allow-auto-update-schema public/default --enable
  kubectl exec -it pod/cluster2-dc1-rack1-sts-0 -- /apache-pulsar-2.8.0/bin/pulsar-admin --admin-url $PULSAR_BROKER_HTTP $PULSAR_ADMIN_AUTH namespaces set-retention public/default --size -1 --time -1
}

source_list() {
  kubectl exec -it pod/cluster2-dc1-rack1-sts-0 -- /apache-pulsar-2.8.0/bin/pulsar-admin --admin-url $PULSAR_BROKER_HTTP $PULSAR_ADMIN_AUTH source list
}

sink_list() {
  kubectl exec -it pod/cluster2-dc1-rack1-sts-0 -- /apache-pulsar-2.8.0/bin/pulsar-admin --admin-url $PULSAR_BROKER_HTTP $PULSAR_ADMIN_AUTH sink list
}

# The connector must be deployed when the keyspace exists
deploy_csc() {
  kubectl exec -it pod/cluster2-dc1-rack1-sts-0 -- /apache-pulsar-2.8.0/bin/pulsar-admin --admin-url $PULSAR_BROKER_HTTP $PULSAR_ADMIN_AUTH source create \
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
      \"key.converter\": \"com.datastax.oss.pulsar.source.converters.NativeAvroConverter\",
      \"value.converter\": \"com.datastax.oss.pulsar.source.converters.NativeAvroConverter\",
      \"contactPoints\": \"$CASSANDRA_SERVICE\",
      \"loadBalancing.localDc\": \"$CASSANDRA_DC\",
      \"auth.provider\": \"PLAIN\",
      \"auth.username\": \"$USERNAME\",
      \"auth.password\": \"$PASSWORD\"
    }"
}

undeploy_csc() {
  kubectl exec -it pod/cluster2-dc1-rack1-sts-0 -- /apache-pulsar-2.8.0/bin/pulsar-admin --admin-url $PULSAR_BROKER_HTTP $PULSAR_ADMIN_AUTH source delete \
    --tenant public \
    --namespace default \
    --name cassandra-source-ks1-table1
}

csc_status() {
  kubectl exec -it pod/cluster2-dc1-rack1-sts-0 -- /apache-pulsar-2.8.0/bin/pulsar-admin --admin-url $PULSAR_BROKER_HTTP $PULSAR_ADMIN_AUTH source status --name cassandra-source-ks1-table1
}

topic_event_stats() {
  kubectl exec -it pod/cluster2-dc1-rack1-sts-0 -- /apache-pulsar-2.8.0/bin/pulsar-admin --admin-url $PULSAR_BROKER_HTTP $PULSAR_ADMIN_AUTH topics stats persistent://public/default/events-ks1.table1
}

topic_data_stats() {
  kubectl exec -it pod/cluster2-dc1-rack1-sts-0 -- /apache-pulsar-2.8.0/bin/pulsar-admin --admin-url $PULSAR_BROKER_HTTP $PULSAR_ADMIN_AUTH topics stats persistent://public/default/data-ks1.table1
}

deploy_es_sink() {
  kubectl exec -it pod/cluster2-dc1-rack1-sts-0 -- /apache-pulsar-2.8.0/bin/pulsar-admin --admin-url $PULSAR_BROKER_HTTP $PULSAR_ADMIN_AUTH sink create \
    --sink-type elastic_search \
    --tenant public \
    --namespace default \
    --name elasticsearch-sink-ks1-table1 \
    --inputs persistent://public/default/data-ks1.table1 \
    --subs-position Earliest \
    --sink-config "{
      \"elasticSearchUrl\":\"$ELASTICSEARCH_URL\",
      \"indexName\":\"ks1.table1\",
      \"keyIgnore\":\"false\",
      \"nullValueAction\":\"DELETE\",
      \"schemaEnable\":\"true\"
    }"
}

undeploy_es_sink() {
  kubectl exec -it pod/cluster2-dc1-rack1-sts-0 -- /apache-pulsar-2.8.0/bin/pulsar-admin --admin-url $PULSAR_BROKER_HTTP $PULSAR_ADMIN_AUTH sink delete \
    --tenant public \
    --namespace default \
    --name elasticsearch-sink-ks1-table1
}

es_sink_status() {
   kubectl exec -it pod/cluster2-dc1-rack1-sts-0 -- /apache-pulsar-2.8.0/bin/pulsar-admin --admin-url $PULSAR_BROKER_HTTP $PULSAR_ADMIN_AUTH sink status --name elasticsearch-sink-ks1-table1
}

es_refresh() {
  kubectl exec -it pod/cluster2-dc1-rack1-sts-0 -- curl -XPOST "$ELASTICSEARCH_URL/ks1.table1/_refresh"
}

es_total_hits() {
  TOTAL_HIT=$(kubectl exec -it pod/cluster2-dc1-rack1-sts-0 -- curl "$ELASTICSEARCH_URL/ks1.table1/_search?pretty&size=0" 2>/dev/null | jq '.hits.total.value')
  if [ "$TOTAL_HIT" != "${1}" ]; then
	     echo "### total_hit : unexpected total.hits = $TOTAL_HIT"
	     return 1
	fi
}

es_source_match() {
  TMP_DIR=$(mktemp -d)
  SOURCE=$(kubectl exec -it pod/cluster2-dc1-rack1-sts-0 -- curl "$ELASTICSEARCH_URL/ks1.table1/_doc/${1}" 2>/dev/null | jq '._source')
  echo "$SOURCE" > $TMP_DIR/source.json
  echo ${2} > $TMP_DIR/expected.json
  if [ $(jd --set $TMP_DIR/expected.json $TMP_DIR/source.json | wc -c) -ne 0 ]; then
	     echo "### es_source_match : _id=${1} unexpected _source = $SOURCE"
	     return 1
	fi
}

run_cqlsh() {
  kubectl exec -it pod/cluster2-dc1-rack1-sts-0 -- cqlsh -u $USERNAME -p $PASSWORD -e "${1}"
}

cleanup_test() {
  undeploy_csc
  undeploy_es_sink
  kubectl exec -it pod/cluster2-dc1-rack1-sts-0 -- curl -XDELETE "$ELASTICSEARCH_URL/ks1.table1"
  cqlsh -u $USERNAME -p $PASSWORD -e "drop KEYSPACE ks1;"
  kubectl exec -it pod/cluster2-dc1-rack1-sts-0 -- /apache-pulsar-2.8.0/bin/pulsar-admin --admin-url $PULSAR_BROKER_HTTP $PULSAR_ADMIN_AUTH topics delete -d -f persistent://public/default/events-ks1.table1
  kubectl exec -it pod/cluster2-dc1-rack1-sts-0 -- /apache-pulsar-2.8.0/bin/pulsar-admin --admin-url $PULSAR_BROKER_HTTP $PULSAR_ADMIN_AUTH topics delete -d -f persistent://public/default/data-ks1.table1
}
