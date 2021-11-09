# Cassandra 4.x CDC agent for Luna Streaming

## Build

    ./gradlew agent-c4-luna:shadowJar

## Run

    export JVM_EXTRA_OPTS="-javaagent:agent-c4-luna/build/libs/agent-c4-luna-<version>-all.jar=pulsarServiceUrl=pulsar://pulsar:6650"
