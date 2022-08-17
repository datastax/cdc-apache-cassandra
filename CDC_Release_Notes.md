# Release notes for CDC for Apache Cassandra&trade;
The CDC for Apache Cassandra&trade; is compatible for Apache Cassandra&trade; 3.11, 4.0, and DSE 6.8.

# Release notes for 2.2.0
17 August 2022

## Changes for Pulsar Connector
* Add a new source option to generate data events in JSON format

# Release notes for 2.1.0
7 June 2022

## Changes for Pulsar Connector
* Upgrade org.json to 20220320
* Fixed an internal error: NoClassDefFoundError: com/datastax/oss/driver/internal/core/util/ArrayUtils

## Changes for Agent
* Upgrade C* dependencies: Cassandra 4.0.4, DSE 6.18.23
* Upgrade Pulsar client to 2.8.3


# Release notes for 2.0.0
26 April 2022

## Changes
* Dropped support for Luna Streaming 2.7

# Release notes for 1.0.5
11 April 2022

## Changes
* Cassandra agent: allow configuring TLS options (`tlsTrustCertsFilePath` and `useKeyStoreTls`).

# Release notes for 1.0.4
05 April 2022

## Changes
* Provide support for C* collection types
* jackson-databind upgrade

# Release notes for 1.0.3
17 March 2022

## Changes
* Add new environment variables for the CDC agent configuration
* Upgrade to DSE 6.8.18
* Fix documentation

# Release notes for 1.0.2
19 January 2022

## Changes
* Upgrade some dependencies

# Release notes for 1.0.1
10 December 2021

## Changes
* Initial release
