# Cassandra 3.x CDC producer for Pulsar

## Build

    ./gradlew producer-v3-pulsar:shadowJar

## Run

    export JVM_EXTRA_OPTS="-javaagent:$CSC_HOME/producer-v3-pulsar/build/libs/producer-<version>-all.jar=pulsarServiceUrl=pulsar://pulsar:6650"
