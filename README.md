# Cassandra CDC Replication using Apache Pulsar or Apache Kafka.

-The Cassandra replication requires:
* A message producer deployed as a JVM agent on each Cassandra data node.
* A Cassandra source connector deployed in the streaming platform.

| Streaming platform | Cassandra v3.x producer | Cassandra v4.x producer  | Source connector |
| ---                | ---                     | ---                      | ---              |
| Apache Pulsar      | [producer-v3-pulsar](producer-v3-pulsar) | [producer-v4-pulsar](producer-v4-pulsar) | [source-pulsar](source-pulsar) |
| Apache Kafka       | [producer-v3-kafka](producer-v3-kafka)   | [producer-v4-kafka](producer-v4-kafka)   | [source-kafka](source-kafka)   |

The following diagram describes the general architecture. The CDC replication ensures the most recent state of the Cassandra table is replicated into the data topic, and the schema in the schema-registry reflects the Cassandra table schema.

![Cassandra-source-connector](docs/images/cassandra-source-connector.png)

## Limitations

* Does not replay batch updates
* Does not manage table truncates
* Does not sync data available before starting the CDC producer.
* Does not track TTL
