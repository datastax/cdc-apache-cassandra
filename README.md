# Cassandra CDC Replication using Apache Pulsar.

The Cassandra Source Connector requires:
* A message producer deployed as a JVM agent on each Cassandra data node.
* A Cassandra Source Connector deployed in the streaming platform.

| Streaming platform | Cassandra v3.x producer | Cassandra v4.x producer  | Source connector |
| ---                | ---                     | ---                      | ---              |
| Apache Pulsar      | [producer-v3-pulsar](producer-v3-pulsar) | [producer-v4-pulsar](producer-v4-pulsar) | [source-pulsar](source-pulsar) |

![Cassandra-source-connector](docs/modules/ROOT/assets/images/cassandra-source-connector.png)

## Documentation

All documentation is available online [here](https://datastax.github.io/cassandra-source-connector/).

## Limitations

* Does not replay logged batches
* Does not manage table truncates
* Does not manage TTLs
* Does not support range deletes
* Does not sync data available before starting the CDC producer.
* CQL column names must not match a Pulsar primitive type name (ex: INT32)

## Supported data types

Cassandra supported CQL3 data types (with the associated AVRO type or logical-type):

* text (string), ascii (string)
* tinyint (int), smallint (int), int (int), bigint (long), double (double), float (float),
* inet (string)
* decimal (cql_decimal), varint (cql_varint), duration (cql_duration)
* blob(bytes)
* boolean (boolean)
* timestamp (timestamp-millis), time (time-micros), date (date)
* uuid, timeuuid (uuid)
* User Defined Types (record)

## Build from the sources

    ./gradlew assemble
