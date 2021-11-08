# Cassandra 3.x CDC agent for Luna Streaming

## Build

    ./gradlew agent-c3-luna:shadowJar

## Run

    export JVM_EXTRA_OPTS="-javaagent:$CSC_HOME/agent-c3-luna/build/libs/agent-c3-luna-<version>-all.jar=pulsarServiceUrl=pulsar://pulsar:6650"
