#!/usr/bin/env bash
set -x

STORAGE_CLASS_NAME="server-storage"
REGISTRY_NAME='kind-registry'
REGISTRY_PORT='5000'

#KIND_IMAGE="kindest/node:v1.15.11@sha256:6cc31f3533deb138792db2c7d1ffc36f7456a06f1db5556ad3b6927641016f50"
KIND_IMAGE="kindest/node:v1.16.9@sha256:7175872357bc85847ec4b1aba46ed1d12fa054c83ac7a8a11f5c268957fd5765"

create_cluster() {
  case "${2:-6}" in
  "3") create_kind_cluster3_with_local_registry ${1:-cluster1}
    ;;
  *) create_kind_cluster6_with_local_registry ${1:-cluster1}
    ;;
  esac
}

# $1 = cluster name
create_kind_cluster3() {
  kind create cluster --name ${1:-cluster1} --config $KIND_DIR/kind-config-worker-3.yaml --image $KIND_IMAGE
  kubectl apply -f integ-test/kind/kind-storageclass.yaml

  # Add zone label
  kubectl label nodes cluster1-worker failure-domain.beta.kubernetes.io/zone=a
  kubectl label nodes cluster1-worker2 failure-domain.beta.kubernetes.io/zone=b
  kubectl label nodes cluster1-worker3 failure-domain.beta.kubernetes.io/zone=c
  kubectl get nodes -o wide -L failure-domain.beta.kubernetes.io/zone,beta.kubernetes.io/instance-type
}

# $1 = cluster name
delete_cluster() {
  kind delete clusters ${1:-cluster1}
}

create_registry() {
  create_local_registry
}

# check registry content:
# http://localhost:5000/v2/_catalog
# http://localhost:5000/v2/strapdata/elassandra-node-dev/tags/list
create_local_registry() {
  running="$(docker inspect -f '{{.State.Running}}' "${REGISTRY_NAME}" 2>/dev/null || true)"
  if [ "${running}" != 'true' ]; then
    docker run \
      -d --restart=always -p "${REGISTRY_PORT}:5000" --name "${REGISTRY_NAME}" \
      registry:2
  fi

  # connect to the kind cluster if created before the registry
  docker network connect "kind" "${REGISTRY_NAME}" || true
}

# create registry container unless it already exists
create_kind_cluster6_with_local_registry() {
  # create a cluster with the local registry enabled in containerd
  cat <<EOF | kind create cluster --name ${1:-cluster1} --image $KIND_IMAGE --config=-
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
- role: worker
- role: worker
- role: worker
- role: worker
- role: worker
- role: worker
containerdConfigPatches:
- |-
  [plugins."io.containerd.grpc.v1.cri".registry.mirrors."localhost:${REGISTRY_PORT}"]
    endpoint = ["http://${REGISTRY_NAME}:${REGISTRY_PORT}"]
EOF

  # connect the registry to the cluster network if the registry exist
  docker network connect "kind" "${REGISTRY_NAME}" || true

  # tell https://tilt.dev to use the registry
  # https://docs.tilt.dev/choosing_clusters.html#discovering-the-registry
  for node in $(kind get nodes --name ${1:-cluster1});
  do
    kubectl annotate node "${node}" "kind.x-k8s.io/registry=localhost:${REGISTRY_PORT}";
  done

  # create storageclass
  kubectl apply -f integ-test/kind/kind-storageclass.yaml

  # add zone label
  kubectl label nodes cluster1-worker failure-domain.beta.kubernetes.io/zone=a
  kubectl label nodes cluster1-worker2 failure-domain.beta.kubernetes.io/zone=b
  kubectl label nodes cluster1-worker3 failure-domain.beta.kubernetes.io/zone=c
  kubectl label nodes cluster1-worker4 failure-domain.beta.kubernetes.io/zone=a
  kubectl label nodes cluster1-worker5 failure-domain.beta.kubernetes.io/zone=b
  kubectl label nodes cluster1-worker6 failure-domain.beta.kubernetes.io/zone=c
}

create_kind_cluster3_with_local_registry() {
  # create a cluster with the local registry enabled in containerd
  cat <<EOF | kind create cluster --name ${1:-cluster1} --image $KIND_IMAGE --config=-
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
- role: worker
- role: worker
- role: worker
containerdConfigPatches:
- |-
  [plugins."io.containerd.grpc.v1.cri".registry.mirrors."localhost:${REGISTRY_PORT}"]
    endpoint = ["http://${REGISTRY_NAME}:${REGISTRY_PORT}"]
EOF

  # connect the registry to the cluster network if the registry exist
  docker network connect "kind" "${REGISTRY_NAME}" || true

  # tell https://tilt.dev to use the registry
  # https://docs.tilt.dev/choosing_clusters.html#discovering-the-registry
  for node in $(kind get nodes --name ${1:-cluster1});
  do
    kubectl annotate node "${node}" "kind.x-k8s.io/registry=localhost:${REGISTRY_PORT}";
  done

  # create storageclass
  kubectl apply -f integ-test/kind/kind-storageclass.yaml

  # add zone label
  kubectl label nodes cluster1-worker failure-domain.beta.kubernetes.io/zone=a
  kubectl label nodes cluster1-worker2 failure-domain.beta.kubernetes.io/zone=b
  kubectl label nodes cluster1-worker3 failure-domain.beta.kubernetes.io/zone=c
}




