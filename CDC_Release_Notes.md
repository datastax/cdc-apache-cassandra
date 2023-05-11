# Release notes for CDC for Apache Cassandra&trade;
The CDC for Apache Cassandra&trade; is compatible for Apache Cassandra&trade; 3.11, 4.0, and DSE 6.8.

# Release notes for 2.2.9
11 May 2023

## Changes
* [improve][agent] Log the dse4-agent version upon startup
* [fix][agent] Properly release all semaphore permits when commit log segment tasks fail partially
* Upgrade org.json to fix CVE-45688
* Improve WatchService error handling & logging
* Add a limitation for primary key only tables
* Disable the replication_latency metric for backfilled items
* Update pulsar dependency to 2.10.3

# Release notes for 2.2.5
10 April 2023

## Changes
* [New] CDC Backfill CLI: a tool for hydrating a CDC data topic with historical Apache Cassandra&trade; table records. See the [README](backfill-cli/README.md) for more details.
* [Cassandra Source Connector]: Update the java-driver-core dependency to 4.15.0 to leverage some advanced driver settings (namely, `advanced.address-translator`)

# Release notes for 2.2.3
2 Feb 2023

## Changes
* Cassandra connector: Fix an internal error: ClassCastException when outputFormat is set to `json` 

# Release notes for 2.2.2
10 Nov 2022

## Changes
* Upgrade JacksonXML to 2.12.7.1

# Release notes for 2.2.1
29 August 2022

## Changes for Pulsar Connector
* Allow optional UDT fields by relaxing the data topic schema 

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
