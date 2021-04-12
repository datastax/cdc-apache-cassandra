# Cassandra 3.x CDC producer for Pulsar

## Build

    ./gradlew producer-v3-pulsar:shadowJar

## Run

    export JVM_EXTRA_OPTS="-javaagent:$CDC_HOME/producer-v3-pulsar/build/libs/producer-0.1-SNAPSHOT-all.jar -DkafkaRegistryUrl=http://schema-registry:8081"
