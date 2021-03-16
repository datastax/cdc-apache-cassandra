# Pulsar-Notes

## Start Pulsar

Start pulsar standalone or as a daemon:
* bin/pulsar standalone
* bin/pulsar-daemon start standalone
* bin/pulsar-daemon stop standalone

## Pulsar GUI

    docker pull apachepulsar/pulsar-manager:v0.2.0
    docker run -d -it \
        -p 9527:9527 -p 7750:7750 \
        -e SPRING_CONFIGURATION_FILE=/pulsar-manager/pulsar-manager/application.properties \
        apachepulsar/pulsar-manager:v0.2.0

Set admin password:

    CSRF_TOKEN=$(curl http://localhost:7750/pulsar-manager/csrf-token)
    curl \
       -H 'X-XSRF-TOKEN: $CSRF_TOKEN' \
       -H 'Cookie: XSRF-TOKEN=$CSRF_TOKEN;' \
       -H "Content-Type: application/json" \
       -X PUT http://localhost:7750/pulsar-manager/users/superuser \
       -d '{"name": "admin", "password": "apachepulsar", "description": "test", "email": "username@test.org"}'


* Connect to http://localhost:9527/
* Create an env with serviceUrl=http://docker.for.mac.host.internal:8080/

## Config

* Create/Delete a partitioned topic

    bin/pulsar-admin topics list public/default
    bin/pulsar-admin topics create-partitioned-topic persistent://public/default/topic1 --partitions 2
    bin/pulsar-admin topics delete persistent://public/default/sub1 --force

* Create a subscription

    bin/pulsar-admin topics create-subscription -s sub1 persistent://public/default/topic1

* enable schema auto-update

    bin/pulsar-admin namespaces set-is-allow-auto-update-schema --enable public/default
    
* schema management

    bin/pulsar-admin schemas get "persistent://public/default/elasticsearch-ks1-sink"
    bin/pulsar-admin schemas delete "persistent://public/default/elasticsearch-ks1-sink"


* Enable message dedup at the broker level

    bin/pulsar-admin namespaces set-deduplication public/default --enable

* Configure Functions-Worker to run with brokers

In broker.conf:
* functionsWorkerEnabled=true

In conf/functions_worker.yml:
* numFunctionPackageReplicas=1 (for demo only)
* pulsarFunctionsCluster=<cluster_name>

## Producer test

    bin/pulsar-client produce persistent://public/default/topic1 -m "1610388451000" -k "1"


## Debug connectors

Enable Functions Worker Service in Broker in conf/broker.conf:

    functionsWorkerEnabled=true

Start pulsar connector with JVM options:

    PULSAR_EXTRA_OPTS="-Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=8001"