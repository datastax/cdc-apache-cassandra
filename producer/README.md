# CDC Producer

Standalone producer that sent mutation read from the Cassandra 4.x commitlogs to a Pulsar topic (or Quasar).

# Run

    ./gradlew clean producer:run
    ./gradlew clean producer:runLocalPulsar