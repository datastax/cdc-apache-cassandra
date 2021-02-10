#!/usr/bin/env bash
#
# Require az cli 2.0.76+
# On  Mac: brew upgrade azure-cli
# az extension update --name aks-preview
# az feature register --name NodePublicIPPreview --namespace Microsoft.ContainerService
# az account set --subscription 72738c1b-8ae6-4f23-8531-5796fe866f2e
set -x

AZURE_REGION=${AZURE_REGION:-"northeurope"}
RESOURCE_GROUP_NAME=${RESOURCE_GROUP_NAME:-"aks1"}
K8S_CLUSTER_NAME=${K8S_CLUSTER_NAME:-"cl1"}

REGISTRY_URL="docker.io"
REGISTRY_NAME="vrtestregistry"
STORAGE_CLASS_NAME="default"
SUBSCRIPTION_ID="72738c1b-8ae6-4f23-8531-5796fe866f2e"
SERVICE_PRINCIPAL_NAME="testuser"

create_cluster() {
  create_resource_group $RESOURCE_GROUP_NAME
  create_aks_cluster_vmss
  #add_nsg_rule
}

delete_cluster() {
  delete_aks_cluster $RESOURCE_GROUP_NAME $K8S_CLUSTER_NAME
  delete_resource_group $RESOURCE_GROUP_NAME
}


# $1 = $RESOURCE_GROUP_NAME
create_resource_group() {
  az group create -l $AZURE_REGION -n $1
}

# $1 = $RESOURCE_GROUP_NAME
delete_resource_group() {
    az group delete -n $1 -y
}

# $1 = $RESOURCE_GROUP_NAME
# $1 = vnet name
create_vnet0() {
  az network vnet create --name vnet0 -g $1 --address-prefix 10.0.0.0/17 --subnet-name subnet0 --subnet-prefix 10.0.0.0/24
}

create_registry() {
  echo "Creating registry=$REGISTRY_NAME"
  az acr create --name $REGISTRY_NAME --resource-group $RESOURCE_GROUP_NAME --sku Basic
  az aks update -n ${K8S_CLUSTER_NAME} -g $RESOURCE_GROUP_NAME --attach-acr $REGISTRY_NAME
}

# see https://thorsten-hans.com/how-to-use-private-azure-container-registry-with-kubernetes
create_azure_registry() {
  az acr create --name $REGISTRY_NAME --resource-group $RESOURCE_GROUP_NAME --sku Basic
}

create-acr-rbac() {
    eval $(az ad sp create-for-rbac \
    --scopes /subscriptions/$SUBSCRIPTION_ID/resourcegroups/$RESOURCE_GROUP_NAME/providers/Microsoft.ContainerRegistry/registries/$REGISTRY_NAME \
    --role Contributor \
    --name $SERVICE_PRINCIPAL_NAME | jq -r '"export SPN_PW=\(.password) && export SPN_CLIENT_ID=\(.appId)"')
    kubectl create secret docker-registry acr-secret \
            --docker-server=$REGISTRY_NAME \
            --docker-username=$SPN_CLIENT_ID \
            --docker-password=$SPN_PW \
            --docker-email=ignorethis@email.com
}

acr-login() {
  az acr login --name $REGISTRY_NAME -u $SPN_CLIENT_ID -p$SPN_PW
}

create_aks_cluster_vmss() {
   az aks create --name "${K8S_CLUSTER_NAME}" \
                  --resource-group $RESOURCE_GROUP_NAME \
                  --network-plugin azure \
                  --node-count 1 \
                  --node-vm-size Standard_D8_v3 \
                  --vm-set-type VirtualMachineScaleSets \
                  --output table \
                  --zone 1 2 3
#                  --enable-node-public-ip

    kubectl create clusterrolebinding kubernetes-dashboard -n kube-system --clusterrole=cluster-admin --serviceaccount=kube-system:kubernetes-dashboard
    use_aks_cluster $RESOURCE_GROUP_NAME "${K8S_CLUSTER_NAME}"
}

# $1 =  VM instance index starting from 0
add_public_vmss_ip() {
   AKS_RG_NAME=$(az resource show --namespace Microsoft.ContainerService --resource-type managedClusters -g $RESOURCE_GROUP_NAME -n $K8S_CLUSTER_NAME | jq -r .properties.nodeResourceGroup)
   AKS_VMSS_INSTANCE=$(kubectl get nodes -o json | jq -r ".items[${1:-0}].metadata.name")
   AKS_VMSS=${AKS_VMSS_0::-6}

   PUBLIC_IP=$(az vmss list-instance-public-ips -g $AKS_RG_NAME -n $AKS_VMSS | jq -r ".[${1:-0}].ipAddress")
   kubectl label nodes --overwrite $AKS_VMSS_0 elassandra.strapdata.com/public-ip=$PUBLIC_IP
}

# $# = inbound tcp ports
add_nsg_rule() {
  AKS_RG_NAME=$(az resource show --namespace Microsoft.ContainerService --resource-type managedClusters -g $RESOURCE_GROUP_NAME -n $K8S_CLUSTER_NAME | jq -r .properties.nodeResourceGroup)
  NSG_NAME=$(az network nsg list -g $AKS_RG_NAME | jq -r .[0].name)
  az network nsg rule create \
    --resource-group $AKS_RG_NAME \
    --nsg-name $NSG_NAME \
    --name elassandra_inbound \
    --description "Elassandra inbound rule" \
    --priority 2000 \
    --access Allow \
    --source-address-prefixes Internet \
    --protocol Tcp \
    --direction Inbound \
    --destination-address-prefixes '*' \
    --destination-port-ranges $@
}


# $1 = RESOURCE_GROUP_NAME
# $2 = K8S_CLUSTER_NAME
delete_aks_cluster() {
    az aks delete  -y --name "${2}" --resource-group $1
}

# $1 = RESOURCE_GROUP_NAME
# $2 = K8S_CLUSTER_NAME
use_aks_cluster() {
   az aks get-credentials --name "${2}" --resource-group $1 --output table
   kubectl config set-context $1 --cluster=${2}
}


