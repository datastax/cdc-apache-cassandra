# Cassandra 4.x CDC producer for Kafka

## Build

    ./gradlew producer-v4-kafka:shadowJar

## Run

    export JVM_EXTRA_OPTS="-javaagent:$CDC_HOME/producer-v4-kafka/build/libs/producer-v4-kafka-0.1-SNAPSHOT-all.jar -DschemaRegistryUrl=http://localhost:8081"

