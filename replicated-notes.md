# Replicated notes

Replicated can deploy a new kubectl cluster including an application (installed on VMs), 
or deploy in an existing k8s cluster.

Replicated can also proxy docker images access, 
and offer a private image registry with app-license access control.

Provides community licenses (or free licenses) for OSS project.
Provides an offline k8s cluster (airgap cluster) installer including all needed images.

# Install the replicated CLI

    brew install replicatedhq/replicated/cli
    replicated version

Set env var (get an API TOKEN from replicated.com, menu Teams):

    export REPLICATED_APP=...
    export REPLICATED_API_TOKEN=...

# Deploy an application

A git branch = a replicated chanel
In the manifest directory, put our YAML deployment configuration files

    replicated release create --auto
    replicated release ls

# Create a customer license

Create a license for a customer+chanel

