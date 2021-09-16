# Cassandra CDC Replication using Apache Pulsar.

The Cassandra Source Connector requires:
* A message producer deployed as a JVM agent on each Cassandra data node.
* A Cassandra Source Connector deployed in the streaming platform.

| Streaming platform | Cassandra v3.x producer | Cassandra v4.x producer  | Source connector |
| ---                | ---                     | ---                      | ---              |
| Apache Pulsar      | [producer-v3-pulsar](producer-v3-pulsar) | [producer-v4-pulsar](producer-v4-pulsar) | [source-pulsar](source-pulsar) |

![Cassandra-source-connector](docs/modules/ROOT/assets/images/cassandra-source-connector.png)

## Documentation

All documentation is available online [here](https://docs.datastax.com/en/cassandra-source-connector/index.html).

## Limitations

* Does not replay logged batches
* Does not manage table truncates
* Does not manage TTLs
* Does not support range deletes
* Does not sync data available before starting the CDC producer.

## Build from the sources

    ./gradlew assemble
