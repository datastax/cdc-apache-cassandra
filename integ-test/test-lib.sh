#!/usr/bin/env bash
#
# Require az cli 2.0.76+
# On  Mac: brew upgrade azure-cli
# az extension update --name aks-preview
# az feature register --name NodePublicIPPreview --namespace Microsoft.ContainerService
# az account set --subscription 72738c1b-8ae6-4f23-8531-5796fe866f2e
set -x

BASE_DIR=integ-test
HELM_REPO=datastax

CASS_OPERATOR_TAG=1.5.1
#ELASSANDRA_OPERATOR_TAG=$(awk -F "=" '/version/ { print $2 }' gradle.properties)
#ELASSANDRA_NODE_TAG=$(head -n 1 docker/supportedElassandraVersions.txt)

test_start() {
  echo "### Starting $1"
  set -x
  set -o pipefail
  trap error ERR
}

test_end() {
  set +e
  trap - ERR
  finish
}

error() {
  echo "ERROR occurs, test FAILED"
  finish
  exit 1
}

finish() {
  for i in "${HELM_RELEASES[@]}"; do
    helm delete --purge $i
  done
  for i in "${NAMESPACES[@]}"; do
    kubectl delete namespace $i
  done
}

setup_flavor() {
  case "$K8S_FLAVOR" in
  "aks")
    echo "Loading AKS library"
    source $BASE_DIR/aks/aks.sh
    ;;
  "gke")
    echo "Loading GKE library"
    source $BASE_DIR/gke/gke.sh
    ;;
  *)
    echo "Loading Kind library"
    source $BASE_DIR/kind/kind.sh
    ;;
  esac
}

setup_cluster() {
  setup_flavor
  create_cluster ${1:-cluster1} ${2:-3}
  create_registry
  init_helm
}

destroy_cluster() {
  setup_flavor
  delete_cluster
}

init_helm() {
  echo "Installing HELM"
  #kubectl create serviceaccount --namespace kube-system tiller
  #kubectl create clusterrolebinding tiller-cluster-rule --clusterrole=cluster-admin --serviceaccount=kube-system:tiller
  # HELM 2 K8s 1.16+ apiVersion issue
  #helm init  --wait --service-account tiller --override spec.selector.matchLabels.'name'='tiller',spec.selector.matchLabels.'app'='helm' --output yaml | sed 's@apiVersion: extensions/v1beta1@apiVersion: apps/v1@' | kubectl apply -f -

  helm repo add bitnami https://charts.bitnami.com/bitnami
  helm repo add datastax https://datastax.github.io/charts
  helm repo add datastax-pulsar https://datastax.github.io/pulsar-helm-chart
  helm repo add elastic https://helm.elastic.co
  helm repo update
  echo "HELM installed"
}

# $1 secret name
# $2 target context
# $3 target namespace
copy_secret_to_context() {
  kubectl get secret $1 --export -o yaml | kubectl apply --context $2 -n $3 -f -
}

# deploy the cass-operator
# $1 = namespace
# $2 = helm settings
install_cass_operator() {
    echo "Installing cass-operator in namespace ${1:-default}"

    local registry=""
    if [ "$REGISTRY_SECRET_NAME" != "" ]; then
       registry=",image.pullSecrets[0]=$REGISTRY_SECRET_NAME"
    fi

    # helm settings
    local args=""
    if [ "$2" != "" ]; then
       args=",$2"
    fi

    helm install cass-operator $args --wait $HELM_REPO/cass-operator
    echo "cass-operator installed"
}

uninstall_cass_operator() {
    helm delete cass-operator
}

# Deploy single node cluster in namespace=cluster_name
# $1 = cluster name
# $2 = cluster
# $3 = datacenter name
# $4 = number of nodes
# $5 = helm settings
install_elassandra_datacenter() {
    local ns=${1:-"default"}
    local cl=${2:-"cl1"}
    local dc=${3:-"dc1"}
    local sz=${4:-"1"}

    local registry=""
    if [ "$REGISTRY_SECRET_NAME" != "" ]; then
       registry=",image.pullSecrets[0]=$REGISTRY_SECRET_NAME"
    fi

    local args=""
    if [ "$5" != "" ]; then
       args=",$5"
    fi

    helm install --namespace "$ns" --name "$ns-$cl-$dc" \
    --set image.repository=$REGISTRY_URL/strapdata/elassandra-node${registry} \
    --set image.tag=$ELASSANDRA_NODE_TAG \
    --set dataVolumeClaim.storageClassName=${STORAGE_CLASS_NAME:-"standard"} \
    --set kibana.enabled="false" \
    --set reaper.enabled="false",reaper.image="$REGISTRY_URL/strapdata/cassandra-reaper:2.1.0-SNAPSHOT-strapkop" \
    --set cassandra.sslStoragePort="39000",cassandra.nativePort="39001" \
    --set elasticsearch.httpPort="39002",elasticsearch.transportPort="39003" \
    --set jvm.jmxPort="39004",jvm.jdb="39005",prometheus.port="39006" \
    --set replicas="$sz"$args \
    --wait $HELM_REPO/elassandra-datacenter
    echo "Datacenter $cl-$dc size=$sz deployed in namespace $ns"
}

uninstall_cass_datacenter() {
    local ns=${1:-"default"}
    local cl=${2:-"cl1"}
    local dc=${3:-"dc1"}
    helm delete --purge "$ns-$cl-$dc"
    echo "Datacenter $cl-$dc uninstalled in namespace $ns"
}

install_pulsar() {
  helm install  -n pulsar --create-namespace -f integ-test/pulsar-dev-values.yaml --wait pulsar  datastax-pulsar/pulsar
}

uninstall_pulsar() {
  helm delete pulsar
}

configure_pulsar() {
  BASTION_POD=$(kubectl get pods -l component=bastion -o jsonpath="{.items[*].metadata.name}" -n pulsar)
  kubectl exec $BASTION_POD -n pulsar -- bin/pulsar-admin topics list public/default
  kubectl exec $BASTION_POD -n pulsar -- bin/pulsar-admin topics create-partitioned-topic persistent://public/default/topic1 --partitions 2
  kubectl exec $BASTION_POD -n pulsar -- bin/pulsar-admin topics create-subscription -s sub1 persistent://public/default/topic1
  kubectl exec $BASTION_POD -n pulsar -- bin/pulsar-admin namespaces set-is-allow-auto-update-schema --enable public/default
  kubectl exec $BASTION_POD -n pulsar -- bin/pulsar-admin namespaces set-deduplication public/default --enable
}

setup_pulsar_portfoward() {
    # setup port forward to pulsar admin on port 8888
  kubectl port-forward -n pulsar $(kubectl get pods -n pulsar -l component=adminconsole -o jsonpath='{.items[0].metadata.name}') 8888:80 &

  # setup port forward po grafana on port 8889
  kubectl port-forward -n pulsar $(kubectl get pods -n pulsar -l app.kubernetes.io/name=grafana -o jsonpath='{.items[0].metadata.name}') 3000:3000 &
}

install_elasticsearch() {
  helm install elastic-operator elastic/eck-operator -n elastic-system --create-namespace
}

create_namespace() {
    echo "Creating namespace $1"
    kubectl create namespace $1
    kubectl config set-context --current --namespace=$1
    echo "Created namespace $1"
}

generate_ca_cert() {
  echo "generating root CA"
  openssl genrsa -out MyRootCA.key 2048
  openssl req -x509 -new -nodes -key MyRootCA.key -sha256 -days 1024 -out MyRootCA.pem
}

generate_client_cert() {
    echo "generate client certificate"
    openssl genrsa -out MyClient1.key 2048
    openssl req -new -key MyClient1.key -out MyClient1.csr
    openssl x509 -req -in MyClient1.csr -CA MyRootCA.pem -CAkey MyRootCA.key -CAcreateserial -out MyClient1.pem -days 1024 -sha256
    openssl pkcs12 -export -out MyClient.p12 -inkey MyClient1.key -in MyClient1.pem -certfile MyRootCA.pem
}

view_cert() {
    openssl x509 -text -in $1
}

deploy_prometheus_operator() {
  helm install $HELM_DEBUG --name my-promop \
    --set prometheus.ingress.enabled=true,prometheus.ingress.hosts[0]="prometheus.${TRAFIK_FQDN}",prometheus.ingress.annotations."kubernetes\.io/ingress\.class"="traefik" \
    --set alertmanager.ingress.enabled=true,alertmanager.ingress.hosts[0]="alertmanager.${TRAFIK_FQDN}",alertmanager.ingress.annotations."kubernetes\.io/ingress\.class"="traefik" \
    --set grafana.ingress.enabled=true,grafana.ingress.hosts[0]="grafana.${TRAFIK_FQDN}",grafana.ingress.annotations."kubernetes\.io/ingress\.class"="traefik" \
    -f integ-test/prometheus-operator-values.yaml \
    stable/prometheus-operator
}

upgrade_prometheus_operator() {
  helm upgrade $HELM_DEBUG \
    --set prometheus.ingress.enabled=true,prometheus.ingress.hosts[0]="prometheus.${TRAFIK_FQDN}",prometheus.ingress.annotations."kubernetes\.io/ingress\.class"="traefik" \
    --set alertmanager.ingress.enabled=true,alertmanager.ingress.hosts[0]="alertmanager.${TRAFIK_FQDN}",alertmanager.ingress.annotations."kubernetes\.io/ingress\.class"="traefik" \
    --set grafana.ingress.enabled=true,grafana.ingress.hosts[0]="grafana.${TRAFIK_FQDN}",grafana.ingress.annotations."kubernetes\.io/ingress\.class"="traefik" \
    -f integ-test/prometheus-operator-values.yaml \
    my-promop stable/prometheus-operator
}

undeploy_prometheus_operator() {
  kubectl delete crd prometheuses.monitoring.coreos.com
  kubectl delete crd prometheusrules.monitoring.coreos.com
  kubectl delete crd servicemonitors.monitoring.coreos.com
  kubectl delete crd podmonitors.monitoring.coreos.com
  kubectl delete crd alertmanagers.monitoring.coreos.com
  kubectl delete crd thanosrulers.monitoring.coreos.com
  helm delete --purge my-promop
}

deploy_traefik() {
  echo "Deploying traefik proxy with DNS fqdn=${1:-$TRAFIK_FQDN}"
  helm install $HELM_DEBUG --name traefik --namespace kube-system \
    --set rbac.enabled=true,debug.enabled=true \
    --set dashboard.enabled=true,dashboard.domain=dashboard.${1:-$TRAFIK_FQDN} \
    --set service.annotations."external-dns\.alpha\.kubernetes\.io/hostname"="*.${1:-$TRAFIK_FQDN}" \
    stable/traefik
  echo "done."
}