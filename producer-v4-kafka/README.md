# Cassandra 4.x CDC producer for Kafka

## Build

    ./gradlew producer-v4-kafka:shadowJar

## Run

Run Cassandra with the producer agent:

    export JVM_EXTRA_OPTS="-javaagent:$CSC_HOME/producer-v4-kafka/build/libs/producer-v4-kafka-0.1.0-SNAPSHOT-all.jar=kafkaBrokers=localhost:9092,kafkaSchemaRegistryUrl=http://localhost:8081"
