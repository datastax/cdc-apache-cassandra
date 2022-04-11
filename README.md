# DataStax CDC for Apache Cassandra

![agents](https://github.com/datastax/cdc-apache-cassandra/actions/workflows/ci-agents.yaml/badge.svg)
![connectors](https://github.com/datastax/cdc-apache-cassandra/actions/workflows/ci-connectors.yaml/badge.svg)
![documentation](https://github.com/datastax/cdc-apache-cassandra/actions/workflows/publish.yml/badge.svg)
![release](https://github.com/datastax/cdc-apache-cassandra/actions/workflows/release.yaml/badge.svg)
[![GitHub release](https://img.shields.io/github/v/release/datastax/cdc-apache-cassandra.svg)](https://github.com/datastax/cdc-apache-cassandra/releases/latest)

The DataStax CDC for Apache Cassandra requires:

* DataStax Change Agent for Apache Cassandra, which is an event producer deployed as a JVM agent on each Cassandra data node.
* Datastax Source Connector for Apache Pulsar, which is source connector deployed in your streaming platform.

![Cassandra-source-connector](docs/modules/ROOT/assets/images/cassandra-source-connector.png)

Supported streaming platform:
* Apache Pulsar 2.8.1+
* Datastax Luna Streaming 2.8.0.1.1.36+

Supported Cassandra version:
* Cassandra 3.11+
* Cassandra 4.0+
* Datastax Enterprise Server 6.8.16+

Note: Only Cassandra 4.0 and DSE 6.8.16+ support the near realtime CDC allowing to replicate data as soon as they are synced on disk.

## Documentation

All documentation is available online [here](https://docs.datastax.com/en/cdc-for-cassandra).

See the [QUICKSTART.md](QUICKSTART.md) page.

## Demo

Cassandra data replicated to Elasticsearch:

* Create a Cassandra table with cdc enabled
* Deploy a Cassandra source and an Elasticsearch sink into Apache Pulsar
* Writes into Cassandra are replicated to Elasticsearch.


[![asciicast](https://asciinema.org/a/kiEYzHQrPWhJR19nZ7tbqrDIX.png)](https://asciinema.org/a/kiEYzHQrPWhJR19nZ7tbqrDIX?speed=2&theme=tango)

## Monitoring

You can collect Cassandra/DSE and Pulsar metrics into Prometheus, and build a Grafana dashboard with:
* The CQL read latency from the Cassandra Source Connector
* The replication latency from the Cassandra Source Connector (computed from the Cassandra writetime)
* The CDC disk space used in the cdc_raw directory (for DSE only)
* The mutation sent throughput from a Cassandra node
* The pulsar events and data topic rate in

![CDC Dashboard](docs/modules/ROOT/assets/images/cdc-dashboard.png)

## Limitations

* Does not replay logged batches
* Does not manage table truncates
* Does not manage TTLs
* Does not support range deletes
* Does not sync data available before starting the CDC agent.
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
* Collection types:
** list (array)
** set (array)
** map (map)

## Build from the sources

    ./gradlew assemble

## Acknowledgments

Apache Cassandra, Apache Pulsar, Cassandra and Pulsar are trademarks of the Apache Software Foundation.
Elasticsearch, is a trademark of Elasticsearch BV, registered in the U.S. and in other countries.
