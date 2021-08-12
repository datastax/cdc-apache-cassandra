# DSE CDC producer for Pulsar

## Build

    ./gradlew producer-dse-pulsar:shadowJar

## Run

    export JVM_EXTRA_OPTS="-javaagent:$CDC_HOME/producer-dse-pulsar/build/libs/producer-dse-pulsar-<version>-SNAPSHOT-all.jar"
