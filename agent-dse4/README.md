# DSE CDC agent for Apache Pulsar

## Build

    ./gradlew agent-dse4:shadowJar

## Run

    export JVM_EXTRA_OPTS="-javaagent:agent-dse4/build/libs/agent-dse4-<version>-SNAPSHOT-all.jar=pulsarServiceUrl=pulsar://pulsar:6650,cdcWorkingDir=/var/lib/cassandra/cdc"


