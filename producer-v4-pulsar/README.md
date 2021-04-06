# Cassandra 4.x CDC producer for Pulsar

## Build

    ./gradlew producer-v4-pulsar

## Run

    export JVM_EXTRA_OPTS="-javaagent:$CDC_HOME/producer-v4-pulsar/build/libs/producer-0.1-SNAPSHOT-all.jar"
