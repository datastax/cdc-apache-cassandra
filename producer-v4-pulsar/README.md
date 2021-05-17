# Cassandra 4.x CDC producer for Pulsar

## Build

    ./gradlew producer-v4-pulsar:shadowJar

## Run

    export JVM_EXTRA_OPTS="-javaagent:$CDC_HOME/producer-v4-pulsar/build/libs/producer-v4-pulsar-0.1.0-SNAPSHOT-all.jar"
