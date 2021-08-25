# DSE4 CDC producer for Pulsar

DSE4 = DSE + near realtime CDC from C* 4

## Build

    ./gradlew producer-dse4-pulsar:shadowJar

## Run

    export JVM_EXTRA_OPTS="-javaagent:$CDC_HOME/producer-dse4-pulsar/build/libs/producer-dse4-pulsar-<version>-SNAPSHOT-all.jar"
