# DSE CDC agent for Apache Pulsar

## Build

    ./gradlew agent-dse4-pulsar:shadowJar

## Run

    export JVM_EXTRA_OPTS="-javaagent:agent-dse4-pulsar/build/libs/agent-dse4-pulsar-<version>-SNAPSHOT-all.jar=pulsarServiceUrl=pulsar://pulsar:6650,cdcWorkingDir=/var/lib/cassandra/cdc"


