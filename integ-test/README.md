# Cassandra CDC Integration tests

These integration tests can run on various Kubernetes by setting the K8S_FLAVOR env variable to :

* kind (default)
* aks
* gke

The test-lib.sh defines K8S agnostic shell functions, while K8S specific shell functions are defined in aks, gke or kind subdirectories.

Example to run tests on GKE:

Create a Kubernetes cluster + HELM setup:
```
K8S_FLAVOR=gke integ-test/setup-cluster.sh
```

Run tests:
```
K8S_FLAVOR=gke integ-test/test-cdc.sh
...
```

Delete the Kubernetes cluster:
```
K8S_FLAVOR=gke integ-test/destroy-cluster.sh
```