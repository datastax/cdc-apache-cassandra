# Cassandra 4.x CDC producer for Kafka

## Build

    ./gradlew producer-v4-kafka:shadowJar

## Run

    export JVM_EXTRA_OPTS=\"-javaagent:"$CDC_HOME/producer-v4-kafka/build/libs/producer-v4-kafka-0.1-SNAPSHOT-all.jar\" -DschemaRegistryUrl=http://localhost:8081"

## Tests

    /usr/bin/kafka-topics --list --zookeeper localhost:2181
    /usr/bin/kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic events-ks1.table1 --time -1 --offsets 1
