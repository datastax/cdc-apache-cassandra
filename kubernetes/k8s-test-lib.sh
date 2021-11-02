#!/usr/bin/env bash
#
# Basic test of the full CDC pipeline from DSE to Elasticsearch through Pulsar,
# running in a k8s pod when DSE, Pulsar and ES are installed.
#
# Requires curl, cqlsh, pulsar-admin, jq, jd (https://github.com/josephburnett/jd), see the Dockerfile.
set -x

KEYSPACE="ks1"
TABLE="table1"
EVENTS_TOPIC="persistent://public/default/events-ks1.table1"
DATA_TOPIC="persistent://public/default/data-ks1.table1"

SOURCE_NAME="cassandra-source-${KEYSPACE}-${TABLE}"
SINK_NAME="elasticsearch-sink-${KEYSPACE}-${TABLE}"
INDEX_NAME="${KEYSPACE}-${TABLE}"

pulsar_configure() {
  kubectl exec -it -n $PULSAR_NAMESPACE $PULSAR_ADMIN_POD -- bin/pulsar-admin namespaces set-auto-topic-creation public/default --enable
  kubectl exec -it -n $PULSAR_NAMESPACE $PULSAR_ADMIN_POD -- bin/pulsar-admin namespaces set-is-allow-auto-update-schema public/default --enable
  kubectl exec -it -n $PULSAR_NAMESPACE $PULSAR_ADMIN_POD -- bin/pulsar-admin namespaces set-retention public/default --size -1 --time -1
}

create_partitioned_topics() {
  kubectl exec -it -n $PULSAR_NAMESPACE $PULSAR_ADMIN_POD -- bin/pulsar-admin  topics create-partitioned-topic $EVENTS_TOPIC --partitions 3
  kubectl exec -it -n $PULSAR_NAMESPACE $PULSAR_ADMIN_POD -- bin/pulsar-admin  topics create-partitioned-topic $DATA_TOPIC --partitions 3
}

delete_partitioned_topics() {
  kubectl exec -it -n $PULSAR_NAMESPACE $PULSAR_ADMIN_POD -- bin/pulsar-admin  topics delete-partitioned-topic $EVENTS_TOPIC
  kubectl exec -it -n $PULSAR_NAMESPACE $PULSAR_ADMIN_POD -- bin/pulsar-admin  topics delete-partitioned-topic $DATA_TOPIC
}

list_topics() {
    kubectl exec -it -n $PULSAR_NAMESPACE $PULSAR_ADMIN_POD -- bin/pulsar-admin  topics list public/default
}

source_list() {
  kubectl exec -it -n $PULSAR_NAMESPACE $PULSAR_ADMIN_POD -- bin/pulsar-admin  source list
}

sink_list() {
  kubectl exec -it -n $PULSAR_NAMESPACE $PULSAR_ADMIN_POD -- bin/pulsar-admin  sink list
}

copy_csc_nar() {
  kubectl cp -n $PULSAR_NAMESPACE source-pulsar/build/libs/pulsar-cassandra-source-${PROJECT_VERSION}.nar $PULSAR_ADMIN_POD:/tmp
}

# The connector must be deployed when the keyspace exists
deploy_csc() {
  kubectl exec -it -n $PULSAR_NAMESPACE $PULSAR_ADMIN_POD -- bin/pulsar-admin source create \
    --archive /tmp/pulsar-cassandra-source-${PROJECT_VERSION}.nar \
    --tenant public \
    --namespace default \
    --name $SOURCE_NAME \
    --destination-topic-name ${DATA_TOPIC} \
    --source-config "{
      \"keyspace\": \"${KEYSPACE}\",
      \"table\": \"${TABLE}\",
      \"events.topic\": \"${EVENTS_TOPIC}\",
      \"events.subscription.name\": \"sub1\",
      \"key.converter\": \"com.datastax.oss.pulsar.source.converters.NativeAvroConverter\",
      \"value.converter\": \"com.datastax.oss.pulsar.source.converters.NativeAvroConverter\",
      \"contactPoints\": \"$CASSANDRA_SERVICE\",
      \"loadBalancing.localDc\": \"$CASSANDRA_DC\",
      \"auth.provider\": \"PLAIN\",
      \"auth.username\": \"$CASSANDRA_USERNAME\",
      \"auth.password\": \"$CASSANDRA_PASSWORD\"
    }"
}

undeploy_csc() {
  kubectl exec -it -n $PULSAR_NAMESPACE $PULSAR_ADMIN_POD -- bin/pulsar-admin  source delete --tenant public --namespace default --name $SOURCE_NAME
}

csc_status() {
  kubectl exec -it -n $PULSAR_NAMESPACE $PULSAR_ADMIN_POD -- bin/pulsar-admin  source status --name $SOURCE_NAME
}

csc_logs() {
  kubectl exec -it -n $PULSAR_NAMESPACE $PULSAR_ADMIN_POD -- cat
}

topic_events_stats() {
  kubectl exec -it -n $PULSAR_NAMESPACE $PULSAR_ADMIN_POD -- bin/pulsar-admin  topics stats $EVENTS_TOPIC
}

topic_data_stats() {
  kubectl exec -it -n $PULSAR_NAMESPACE $PULSAR_ADMIN_POD -- bin/pulsar-admin  topics stats $DATA_TOPIC
}

deploy_es_sink() {
  kubectl exec -it -n $PULSAR_NAMESPACE $PULSAR_ADMIN_POD -- bin/pulsar-admin  sink create \
    --sink-type elastic_search \
    --tenant public \
    --namespace default \
    --name $SINK_NAME \
    --inputs $DATA_TOPIC \
    --subs-position Earliest \
    --sink-config "{
      \"elasticSearchUrl\":\"$ELASTICSEARCH_URL\",
      \"indexName\":\"$INDEX_NAME\",
      \"keyIgnore\":\"false\",
      \"nullValueAction\":\"DELETE\",
      \"schemaEnable\":\"true\"
    }"
}

undeploy_es_sink() {
  kubectl exec -it -n $PULSAR_NAMESPACE $PULSAR_ADMIN_POD -- bin/pulsar-admin  sink delete --tenant public --namespace default --name $SINK_NAME
}

es_sink_status() {
   kubectl exec -it -n $PULSAR_NAMESPACE $PULSAR_ADMIN_POD -- bin/pulsar-admin  sink status --name $SINK_NAME
}

es_refresh() {
  kubectl exec -it pod/cluster2-dc1-rack1-sts-0 -- curl -XPOST "$ELASTICSEARCH_URL/$INDEX_NAME/_refresh"
}

es_total_hits() {
  TOTAL_HIT=$(kubectl exec -it pod/cluster2-dc1-rack1-sts-0 -- curl "$ELASTICSEARCH_URL/$INDEX_NAME/_search?pretty&size=0" 2>/dev/null | jq '.hits.total.value')
  if [ "$TOTAL_HIT" != "${1}" ]; then
	     echo "### total_hit : unexpected total.hits = $TOTAL_HIT"
	     return 1
	fi
}

es_source_match() {
  TMP_DIR=$(mktemp -d)
  SOURCE=$(kubectl exec -it pod/cluster2-dc1-rack1-sts-0 -- curl "$ELASTICSEARCH_URL/$INDEX_NAME/_doc/${1}" 2>/dev/null | jq '._source')
  echo "$SOURCE" > $TMP_DIR/source.json
  echo ${2} > $TMP_DIR/expected.json
  if [ $(jd --set $TMP_DIR/expected.json $TMP_DIR/source.json | wc -c) -ne 0 ]; then
	     echo "### es_source_match : _id=${1} unexpected _source = $SOURCE"
	     return 1
	fi
}

run_cqlsh() {
  kubectl exec -it pod/cluster2-dc1-rack1-sts-0 -- cqlsh -u $CASSANDRA_USERNAME -p $CASSANDRA_PASSWORD -e "${1}"
}

cleanup_test() {
  undeploy_csc
  undeploy_es_sink
  kubectl exec -it pod/cluster2-dc1-rack1-sts-0 -- curl -XDELETE "$ELASTICSEARCH_URL/$INDEX_NAME"
  run_cqlsh "drop KEYSPACE ks1;"
  kubectl exec -it -n $PULSAR_NAMESPACE $PULSAR_ADMIN_POD -- bin/pulsar-admin  topics delete -d -f $EVENTS_TOPIC
  kubectl exec -it -n $PULSAR_NAMESPACE $PULSAR_ADMIN_POD -- bin/pulsar-admin  topics delete -d -f $DATA_TOPIC
}
