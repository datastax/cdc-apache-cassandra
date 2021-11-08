# DSE CDC agent for Luna Streaming

## Build

    ./gradlew agent-dse4-luna:shadowJar

## Run

    export JVM_EXTRA_OPTS="-javaagent:$CSC_HOME/agent-dse4-luna/build/libs/agent-dse4-luna-<version>-SNAPSHOT-all.jar=pulsarServiceUrl=pulsar://pulsar:6650,cdcWorkingDir=/var/lib/cassandra/cdc"


