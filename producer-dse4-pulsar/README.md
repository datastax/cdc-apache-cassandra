# DSE CDC producer for Pulsar

## Build

    ./gradlew producer-dse4-pulsar:shadowJar

## Run

    export JVM_EXTRA_OPTS="-javaagent:$CSC_HOME/producer-dse4-pulsar/build/libs/producer-dse4-pulsar-<version>-SNAPSHOT-all.jar=pulsarServiceUrl=pulsar://pulsar:6650,cdcWorkingDir=/var/lib/cassandra/cdc"


