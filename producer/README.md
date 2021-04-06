# CDC Producer

Standalone producer that sent mutation read from the Cassandra 4.x commitlogs to a Pulsar topic (or Quasar).

# Build

    ./gradlew producer:shadowJar
# Run

export JVM_EXTRA_OPTS="-javaagent:$CDC_HOME/producer/build/libs/producer-0.1-SNAPSHOT-all.jar"
