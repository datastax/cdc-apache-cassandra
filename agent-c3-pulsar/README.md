# Cassandra 3.x CDC agent for Luna Streaming

## Build

    ./gradlew agent-c3-pulsar:shadowJar

## Run

    export JVM_EXTRA_OPTS="-javaagent:agent-c3-pulsar/build/libs/agent-c3-pulsar-<version>-all.jar=pulsarServiceUrl=pulsar://pulsar:6650"
